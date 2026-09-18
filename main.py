import asyncio
import os
import shutil
import tempfile
from typing import List, Dict, Optional
import time

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.pipeline.context_utils import call_event_hook
from astrbot.core.star.star_handler import EventType

from .core.utils import (
    get_group_config,
    is_msg_still_available,
    react_to_msg,
    find_file_component,
    build_notification_text,
    purify_file_name,
    backup_file_to_session,
    detect_backend_type,
    is_action_success,
    format_seconds,
)
from .core.checker import CheckerManager
from .core.preview import PreviewManager


TEMP_ENTRY_RETENTION_SECONDS = 24 * 60 * 60


class GroupFileCheckerPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self._backend_type = "napcat"
        self._backend_client_id: int | None = None
        self._backend_detection_lock = asyncio.Lock()

        # 全局配置
        global_settings = self.config.get("global_settings", {})
        self.group_whitelist: List[int] = global_settings.get("group_whitelist", [])
        self.group_whitelist = [int(gid) for gid in self.group_whitelist]
        self.file_size_threshold_mb: int = global_settings.get(
            "file_size_threshold_mb", 100
        )

        # 7za 支持的压缩格式
        self.supported_archive_formats = (
            ".zip",
            ".7z",
            ".tar",
            ".gz",
            ".bz2",
            ".xz",
            ".tar.gz",
            ".tgz",
            ".tar.bz2",
            ".tbz2",
            ".tar.xz",
            ".txz",
            ".iso",
            ".wim",
            ".rar",
        )

        # 支持文本预览的文件格式
        self.supported_text_formats = (
            ".txt",
            ".md",
            ".log",
            ".json",
            ".xml",
            ".yaml",
            ".yml",
            ".ini",
            ".conf",
            ".cfg",
            ".toml",
            ".py",
            ".js",
            ".java",
            ".c",
            ".cpp",
            ".h",
            ".go",
            ".rs",
            ".php",
            ".rb",
            ".sh",
            ".bash",
            ".html",
            ".htm",
            ".css",
            ".jsx",
            ".tsx",
            ".ts",
            ".vue",
            ".sql",
            ".csv",
            ".properties",
            ".env",
        )

        # 图片转换大小限制
        self.image_convert_max_size_mb = 15

        self.temp_dir = os.path.join(
            StarTools.get_data_dir("astrbot_plugin_file_checker"), "temp"
        )
        os.makedirs(self.temp_dir, exist_ok=True)
        self._cleanup_stale_temp_entries()

        # 文件检查间隔控制
        self.check_interval = 0.3
        self.last_check_time = None

        self.download_semaphore = asyncio.Semaphore(5)

        # 初始化管理器
        self.checker = CheckerManager(self)
        self.preview = PreviewManager(self)

        asyncio.create_task(self.checker._load_cache_from_kv())

        logger.info("QQ 文件预览插件已加载。")

    def _cleanup_stale_temp_entries(self) -> None:
        """清理上次进程遗留且已超过保留时间的临时文件。"""
        cutoff = time.time() - TEMP_ENTRY_RETENTION_SECONDS
        removed_count = 0

        try:
            with os.scandir(self.temp_dir) as entries:
                for entry in entries:
                    try:
                        if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            shutil.rmtree(entry.path)
                        else:
                            os.remove(entry.path)
                        removed_count += 1
                    except FileNotFoundError:
                        continue
                    except OSError as e:
                        logger.warning(f"清理过期临时文件失败 {entry.path}: {e}")
        except OSError as e:
            logger.warning(f"扫描临时目录失败 {self.temp_dir}: {e}")

        if removed_count:
            logger.info(f"启动时清理了 {removed_count} 个过期临时文件/目录")

    def _remove_temp_path(self, path: Optional[str], group_id: Optional[int] = None) -> None:
        """安全删除插件临时目录下的文件或目录。"""
        if not path:
            return

        try:
            temp_root = os.path.realpath(self.temp_dir)
            target = os.path.realpath(path)
            if os.path.commonpath([temp_root, target]) != temp_root:
                logger.warning(f"拒绝删除临时目录外的路径: {path}")
                return
        except ValueError:
            logger.warning(f"拒绝删除无法校验的临时路径: {path}")
            return

        try:
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            if group_id is not None:
                logger.debug(f"[{group_id}] 🗑️ 已清理临时路径: {path}")
        except FileNotFoundError:
            pass
        except OSError as e:
            prefix = f"[{group_id}] " if group_id is not None else ""
            logger.warning(f"{prefix}删除临时路径失败 {path}: {e}")

    def _copy_to_temp_dir(self, source_path: str, file_name: str) -> str:
        """将源文件复制到插件临时目录并返回唯一临时路径。"""
        suffix = os.path.splitext(os.path.basename(file_name))[1]
        fd, local_path = tempfile.mkstemp(
            prefix="file_", suffix=suffix, dir=self.temp_dir
        )
        os.close(fd)
        try:
            shutil.copy2(source_path, local_path)
        except Exception:
            self._remove_temp_path(local_path)
            raise
        return local_path

    async def get_cached_file_listing(
        self, group_id: int, client
    ) -> Optional[Dict[str, dict]]:
        """公开 API：获取群文件 flat_index 缓存（file_id → file_info）。

        供其他插件调用，避免重复递归扫描。
        - group_id: QQ 群号
        - client: OneBot 客户端实例
        返回 flat_index dict，失败时返回 None。
        """
        gid = str(group_id)
        try:
            await self._ensure_backend_detected(client)

            # Phase 1: 快速检查，持有锁
            async with self.checker._cache_lock:
                is_cold = gid not in self.checker._cache
                if is_cold:
                    if await self.checker._load_cache_from_kv_for_group(gid):
                        is_cold = False

            # Phase 2: 全量扫描需释放锁，避免阻塞其他协程
            if is_cold:
                scan_result = await self.checker._do_full_scan_group(
                    group_id, client, self._backend_type
                )
                async with self.checker._cache_lock:
                    if gid not in self.checker._cache:
                        self.checker._cache[gid] = scan_result
                return scan_result.get("flat_index", {})
            else:
                async with self.checker._cache_lock:
                    await self.checker._ensure_cache_fresh_group(
                        group_id, client, self._backend_type
                    )
                    self.checker._patch_relative_path(self.checker._cache[gid])
                    return self.checker._cache[gid].get("flat_index", {})
        except Exception as e:
            logger.error(f"[get_cached_file_listing] group={gid} 出错: {e}")
            return None

    @filter.on_decorating_result()
    async def on_bot_sending_file(self, event: AstrMessageEvent):
        """发送消息前：净化 Bot 发送的文件名，并预复制文件到插件目录"""
        result = event.get_result()
        if not result or not result.chain:
            return

        # 仅处理群聊消息，且检查白名单和全局开关
        group_id = event.get_group_id()
        if not group_id:
            return
        if self.group_whitelist and int(group_id) not in self.group_whitelist:
            return
        if not self.config.get("global_settings", {}).get("process_bot_files", False):
            return

        for comp in result.chain:
            if isinstance(comp, Comp.File):
                # 文件名净化
                group_id = str(event.get_group_id())
                check_config = get_group_config(self.config, group_id, "check_module")
                purify_rules = check_config.get("purify_rules", [])

                original_name = comp.name
                purified_name = purify_file_name(original_name, purify_rules)

                if purified_name != original_name:
                    comp.name = purified_name
                    logger.info(
                        f"[{group_id}] Bot 发送文件已净化: {original_name} -> {purified_name}"
                    )

                event.set_extra("_is_bot_sent_file", True)

                # 预复制文件到插件目录，供发送后钩子使用（对齐用户文件的下载流程）
                if comp.file and os.path.exists(comp.file):
                    local_path = self._copy_to_temp_dir(comp.file, comp.name)
                    event.set_extra("_bot_file_local_path", local_path)
                    logger.debug(
                        f"[{group_id}] Bot 文件已预复制到插件目录: {local_path}"
                    )

    @filter.after_message_sent()
    async def on_bot_file_sent(self, event: AstrMessageEvent):
        """发送消息后处理 Bot 文件，并兜底清理异常流程留下的副本。"""
        try:
            await self._on_bot_file_sent_impl(event)
        except Exception:
            self._remove_temp_path(
                event.get_extra("_bot_file_local_path"), int(event.get_group_id())
            )
            raise

    async def _on_bot_file_sent_impl(self, event: AstrMessageEvent):
        """发送消息后：启动 Bot 文件的检查流程（和 on_group_message 对齐）"""
        if not event.get_extra("_is_bot_sent_file"):
            return

        if event.get_extra("_is_repack_file"):
            logger.debug(f"[{event.get_group_id()}] 补档文件，跳过检查流程")
            self._remove_temp_path(
                event.get_extra("_bot_file_local_path"), int(event.get_group_id())
            )
            return
        group_id = str(event.get_group_id())

        result = event.get_result()
        if not result or not result.chain:
            self._remove_temp_path(
                event.get_extra("_bot_file_local_path"), int(group_id)
            )
            return
        check_config = get_group_config(self.config, group_id, "check_module")
        preview_config = get_group_config(self.config, group_id, "preview_module")
        repack_config = get_group_config(self.config, group_id, "repack_module")
        backup_config = get_group_config(self.config, group_id, "backup_module")

        for comp in result.chain:
            if isinstance(comp, Comp.File):
                file_name = comp.name
                upload_time = int(time.time())

                # 查重（和 on_group_message 对齐：刷新缓存 → 查重 → 策略）
                strategy = check_config.get("duplicate_check_strategy", "disabled")
                needs_cache = strategy != "disabled"
                if needs_cache:
                    # 获取文件大小（本地文件或预复制文件）
                    file_size = None
                    if comp.file and os.path.exists(comp.file):
                        file_size = os.path.getsize(comp.file)
                    else:
                        lp = event.get_extra("_bot_file_local_path")
                        if lp and os.path.exists(lp):
                            file_size = os.path.getsize(lp)

                    confirmed = await self.checker._get_duplicate_confirmed(
                        event, group_id, file_size, upload_time
                    )
                    if confirmed:
                        reply_text = self._build_duplicate_notice_text(
                            file_name, confirmed, strategy, delete_delay=600
                        )
                        await self._send_result_with_hooks(
                            event,
                            event.chain_result(
                                [
                                    Comp.Reply(id=event.message_obj.message_id),
                                    Comp.Plain(reply_text),
                                ]
                            ),
                        )
                        if strategy == "delete_old":
                            asyncio.create_task(
                                self.checker._delete_group_file_list(event, confirmed)
                            )
                        elif strategy == "delete_new":
                            new_file_id = await self.checker._search_file_id_by_name(
                                event, file_name, target_time=upload_time
                            )
                            asyncio.create_task(
                                self.checker._delayed_delete_file(
                                    event,
                                    file_name,
                                    600,
                                    upload_time,
                                    file_id=new_file_id,
                                )
                            )
                        self._remove_temp_path(
                            event.get_extra("_bot_file_local_path"), int(group_id)
                        )
                        return

                # 搜索 file_id（缓存已刷新，优先命中）
                file_id = await self.checker._search_file_id_by_name(
                    event, file_name, target_time=upload_time
                )
                if not file_id:
                    logger.warning(f"[{group_id}] Bot 文件未找到: {file_name}")
                    self._remove_temp_path(
                        event.get_extra("_bot_file_local_path"), int(group_id)
                    )
                    continue

                local_path = event.get_extra("_bot_file_local_path")
                if not local_path:
                    logger.debug(
                        f"[{group_id}] Bot 文件 '{file_name}' 无预复制路径，跳过本地处理"
                    )

                asyncio.create_task(
                    self._send_file_check_flow_results(
                        event,
                        self._handle_file_check_flow(
                            event,
                            file_name,
                            file_id,
                            None,
                            None,
                            upload_time,
                            check_config,
                            preview_config,
                            repack_config,
                            backup_config,
                            local_path=local_path,
                            is_bot_file=True,
                            target_msg_id=event.message_obj.message_id,
                        ),
                    )
                )

    async def _ensure_backend_detected(self, client) -> None:
        if client is None or not hasattr(client, "api"):
            return

        client_id = id(client)
        if self._backend_client_id == client_id:
            return

        async with self._backend_detection_lock:
            if self._backend_client_id == client_id:
                return

            try:
                self._backend_type, app_name = await detect_backend_type(client)
                logger.debug(
                    f"[file_checker] 懒探测协议端结果: app_name={app_name or 'unknown'}, "
                    f"backend={self._backend_type}"
                )
            except Exception as e:
                self._backend_type = "napcat"
                logger.warning(
                    f"[file_checker] 懒探测协议端失败，默认按 NapCat 处理: {e}"
                )
            self._backend_client_id = client_id

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=2)
    async def on_group_message(self, event: AstrMessageEvent, *args, **kwargs):
        """处理群文件上传事件"""
        await self._ensure_backend_detected(getattr(event, "bot", None))
        group_id = str(event.get_group_id())
        if self.group_whitelist and int(group_id) not in self.group_whitelist:
            return

        # 获取当前群的配置
        check_config = get_group_config(self.config, group_id, "check_module")
        preview_config = get_group_config(self.config, group_id, "preview_module")
        repack_config = get_group_config(self.config, group_id, "repack_module")
        backup_config = get_group_config(self.config, group_id, "backup_module")

        try:
            raw_event_data = event.message_obj.raw_message
            message_list = raw_event_data.get("message")
            if not isinstance(message_list, list):
                return
            for segment_dict in message_list:
                if (
                    isinstance(segment_dict, dict)
                    and segment_dict.get("type") == "file"
                ):
                    data_dict = segment_dict.get("data", {})
                    file_name = data_dict.get("file")
                    file_id = data_dict.get("file_id")
                    file_size = data_dict.get("file_size")

                    if isinstance(file_size, str):
                        try:
                            file_size = int(file_size)
                        except ValueError:
                            file_size = None

                    if file_name and file_id:
                        # 文件名净化
                        purify_rules = check_config.get("purify_rules", [])
                        original_file_name = file_name
                        purified_file_name = purify_file_name(file_name, purify_rules)

                        # 如果净化后文件名有变化，执行重命名
                        if purified_file_name != original_file_name:
                            try:
                                # 获取文件所属目录
                                current_parent = data_dict.get("parent", "/")
                                rename_result = await event.bot.api.call_action(
                                    "rename_group_file",
                                    group_id=int(group_id),
                                    file_id=file_id,
                                    current_parent_directory=current_parent,
                                    new_name=purified_file_name,
                                )
                                if not is_action_success(rename_result):
                                    raise RuntimeError(
                                        rename_result.get("wording")
                                        or "rename_group_file failed"
                                    )
                                logger.info(
                                    f"[{group_id}] 文件名已净化并重命名: {original_file_name} -> {purified_file_name}"
                                )
                                file_name = (
                                    purified_file_name  # 使用净化后的文件名继续处理
                                )
                            except Exception as e:
                                logger.warning(f"[{group_id}] 文件重命名失败: {e}")
                                # 重命名失败不影响后续流程，使用原文件名继续

                        # 全局大小阈值检查
                        if file_size is not None and self.file_size_threshold_mb > 0:
                            file_size_mb = file_size / (1024 * 1024)
                            if file_size_mb > self.file_size_threshold_mb:
                                logger.debug(
                                    f"[{group_id}] 文件 '{file_name}' 超过全局阈值，跳过处理。"
                                )
                                return

                        file_component = find_file_component(event)
                        if not file_component:
                            return

                        upload_time = raw_event_data.get("time", int(time.time()))

                        gid = str(group_id)
                        strategy = check_config.get(
                            "duplicate_check_strategy", "disabled"
                        )

                        # ===== 缓存与查重流程（仅查重启用时需要缓存） =====
                        needs_cache = strategy != "disabled"
                        if needs_cache:
                            confirmed = await self.checker._get_duplicate_confirmed(
                                event, gid, file_size, upload_time
                            )
                            logger.debug(
                                f"[{gid}] 查重复核: {'确认重复 x' + str(len(confirmed)) if confirmed else '无重复'}"
                            )
                            if confirmed:
                                reply_text = self._build_duplicate_notice_text(
                                    file_name, confirmed, strategy, delete_delay=600
                                )
                                yield event.chain_result(
                                    [
                                        Comp.Reply(id=event.message_obj.message_id),
                                        Comp.Plain(reply_text),
                                    ]
                                )
                                if strategy == "delete_old":
                                    asyncio.create_task(
                                        self.checker._delete_group_file_list(
                                            event, confirmed
                                        )
                                    )
                                elif strategy == "delete_new":
                                    asyncio.create_task(
                                        self.checker._delayed_delete_file(
                                            event,
                                            file_name,
                                            600,
                                            upload_time,
                                            file_id=file_id,
                                        )
                                    )
                                break

                        # Phase 4: 核心检查流程
                        async for result in self._handle_file_check_flow(
                            event,
                            file_name,
                            file_id,
                            file_component,
                            file_size,
                            upload_time,
                            check_config,
                            preview_config,
                            repack_config,
                            backup_config,
                        ):
                            yield result
                        break
        except Exception as e:
            logger.error(f"处理消息时发生致命错误: {e}", exc_info=True)

    async def _send_file_check_flow_results(self, event: AstrMessageEvent, flow):
        async for result in flow:
            await self._send_result_with_hooks(event, result)

    async def _send_result_with_hooks(self, event: AstrMessageEvent, result):
        previous_result = event.get_result()
        event.set_result(result)
        try:
            if await call_event_hook(event, EventType.OnDecoratingResultEvent):
                return
            decorated_result = event.get_result()
            if not decorated_result or not decorated_result.chain:
                return
            await event.send(decorated_result.derive(decorated_result.chain))
            await call_event_hook(event, EventType.OnAfterMessageSentEvent)
        finally:
            if previous_result is None:
                event.clear_result()
            else:
                event.set_result(previous_result)

    def _build_duplicate_notice_text(
        self,
        file_name: str,
        existing_files: list[dict],
        strategy: str = "notify_only",
        delete_delay: int = 300,
    ) -> str:
        count = len(existing_files)

        if count == 1:
            f = existing_files[0]
            msg = (
                f"💡 提醒：文件「{file_name}」可能与群文件中的「{f.get('file_name')}」重复。\n"
                f"  ↳ 上传者: {f.get('uploader_name', '未知')}\n"
                f"  ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))}\n"
                f"  ↳ 所属文件夹: {f.get('parent_folder_name', '根目录')}"
            )
            if strategy == "delete_old":
                msg += "\n\u200b\n该旧文件将被自动清理。"
            elif strategy == "delete_new":
                msg += f"\n\u200b\n此文件将在 {format_seconds(delete_delay)}后删除。"
            return msg

        msg = f"💡 提醒：文件「{file_name}」可能与群文件中以下 {count} 个文件重复：\n"
        for idx, f in enumerate(existing_files, 1):
            msg += (
                f"\n{idx}. {f.get('file_name')}\n"
                f"    ↳ 上传者: {f.get('uploader_name', '未知')}\n"
                f"    ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))}\n"
                f"    ↳ 所属文件夹: {f.get('parent_folder_name', '根目录')}"
            )
        if strategy == "delete_old":
            msg += f"\n\u200b\n以上 {count} 个旧文件将被自动清理。"
        elif strategy == "delete_new":
            msg += f"\n\u200b\n此文件将在 {format_seconds(delete_delay)}后删除。"
        return msg

    async def _handle_file_check_flow(
        self,
        event: AstrMessageEvent,
        file_name: str,
        file_id: str,
        file_component: Optional[Comp.File],
        file_size: Optional[int],
        upload_time: Optional[int],
        check_config: dict,
        preview_config: dict,
        repack_config: dict,
        backup_config: dict,
        *,
        local_path: Optional[str] = None,
        is_bot_file: bool = False,
        target_msg_id: Optional[str] = None,
    ):
        """执行文件检查流程，并确保所有临时副本最终进入清理队列。"""
        cleanup_state = {"path": local_path}
        try:
            async for result in self._handle_file_check_flow_impl(
                event,
                file_name,
                file_id,
                file_component,
                file_size,
                upload_time,
                check_config,
                preview_config,
                repack_config,
                backup_config,
                local_path=local_path,
                is_bot_file=is_bot_file,
                target_msg_id=target_msg_id,
                cleanup_state=cleanup_state,
            ):
                yield result
        finally:
            cleanup_path = cleanup_state["path"]
            if cleanup_path:
                cleanup_delay = check_config.get("check_delay_seconds", 300) * 2
                asyncio.create_task(
                    self._delayed_cleanup_local_path(
                        cleanup_path, cleanup_delay, int(event.get_group_id())
                    )
                )

    async def _handle_file_check_flow_impl(
        self,
        event: AstrMessageEvent,
        file_name: str,
        file_id: str,
        file_component: Optional[Comp.File],
        file_size: Optional[int],
        upload_time: Optional[int],
        check_config: dict,
        preview_config: dict,
        repack_config: dict,
        backup_config: dict,
        *,
        local_path: Optional[str] = None,
        is_bot_file: bool = False,
        target_msg_id: Optional[str] = None,
        cleanup_state: Optional[Dict[str, Optional[str]]] = None,
    ):
        group_id = int(event.get_group_id())
        sender_id = event.get_sender_id()
        self_id = event.get_self_id()
        target_msg_id = target_msg_id or event.message_obj.message_id
        if not is_bot_file and sender_id == self_id:
            logger.debug(f"[{group_id}] 机器人发送的文件，直接跳过处理。")
            return

        # 频率控制
        if self.last_check_time is not None:
            diff = time.time() - self.last_check_time
            if diff < self.check_interval:
                await asyncio.sleep(self.check_interval - diff)
        self.last_check_time = time.time()

        # 等待预检延时
        await asyncio.sleep(check_config.get("pre_check_delay_seconds", 5))

        # 消息存续检查
        if not await is_msg_still_available(event, target_msg_id):
            return

        is_valid = await self.checker._check_validity_via_gfs(event, file_id)
        enable_emoji = check_config.get("enable_emoji", True) and not is_bot_file
        if not is_valid:
            await asyncio.sleep(1)
            retry_valid = await self.checker._check_validity_via_gfs(event, file_id)
            is_valid = retry_valid

        # 统一文件生命周期管理：集中判断、统一下载、统一清理
        pdf_preview_file = None
        pdf_preview_images = []
        try:
            # 1. 聚合判断是否需要下载文件
            needs_download = self._should_download_file(
                file_name, file_size, preview_config, repack_config, backup_config
            )

            # 2. 统一下载：先由框架下载到临时位置，再复制到插件 temp_dir
            if needs_download and not local_path:
                async with self.download_semaphore:
                    assert file_component is not None
                    framework_temp_path = await file_component.get_file()
                # 复制到插件自己的 temp_dir，使用唯一临时路径，避免并发任务互相覆盖
                local_path = self._copy_to_temp_dir(framework_temp_path, file_name)
                if cleanup_state is not None:
                    cleanup_state["path"] = local_path
                logger.debug(f"[{group_id}] 文件已复制到插件 temp_dir: {local_path}")

            # 3. 预览生成
            effective_file_size = file_size
            if (
                effective_file_size is None
                and local_path
                and os.path.exists(local_path)
            ):
                effective_file_size = os.path.getsize(local_path)

            preview_text, extra_info = await self.preview._get_preview_for_file(
                file_name, local_path, effective_file_size, preview_config
            )

            # 4. PDF 预览图生成
            if preview_text.startswith("PDF_PATH:"):
                # 压缩包内返回的 PDF 路径，无论是否生成图片都需要清理
                pdf_preview_file = preview_text[9:]  # 去掉 'PDF_PATH:' 前缀
                preview_text = ""  # 清空预览文本
                if (
                    pdf_preview_file
                    and os.path.exists(pdf_preview_file)
                    and preview_config.get("pdf_preview_pages", 0) > 0
                ):
                    try:
                        pdf_preview_images = await self.preview._get_pdf_preview(
                            pdf_preview_file, preview_config
                        )
                    except Exception as e:
                        logger.error(f"PDF预览处理出错: {e}", exc_info=True)
            elif (
                self.preview._is_pdf_file(file_name)
                and preview_config.get("pdf_preview_pages", 0) > 0
                and local_path
            ):
                # 外层 PDF 文件（local_path 已下载）
                try:
                    pdf_preview_images = await self.preview._get_pdf_preview(
                        local_path, preview_config
                    )
                except Exception as e:
                    logger.error(f"PDF预览处理出错: {e}", exc_info=True)

            if is_valid:
                has_any_preview = bool(preview_text or pdf_preview_images)
                if not is_bot_file:
                    await react_to_msg(
                        event, "314" if has_any_preview else "320", enable_emoji
                    )

                # 5. 自动转换媒体
                if self.preview._is_video_file(file_name):
                    limit = preview_config.get("auto_convert_video_threshold_mb", 0)
                    if (
                        limit > 0
                        and effective_file_size
                        and (effective_file_size / (1024 * 1024)) <= limit
                    ):
                        async for r in self.preview._convert_file_to_media(
                            event, file_name, effective_file_size, local_path, "video"
                        ):
                            yield r

                if self.preview._is_image_file(file_name) and preview_config.get(
                    "enable_auto_convert_image", False
                ):
                    if (
                        effective_file_size
                        and (effective_file_size / (1024 * 1024))
                        <= self.image_convert_max_size_mb
                    ):
                        async for r in self.preview._convert_file_to_media(
                            event, file_name, effective_file_size, local_path, "image"
                        ):
                            yield r

                # 6. 发送通知文案
                if check_config.get("notify_on_success", True) or has_any_preview:
                    success_msg = build_notification_text(
                        file_name, True, preview_text, extra_info, preview_config
                    )
                    if pdf_preview_images:
                        for msg in self.preview.send_pdf_preview(
                            event, success_msg, pdf_preview_images
                        ):
                            yield msg
                    else:
                        chain = []
                        if not is_bot_file:
                            chain.append(Comp.Reply(id=target_msg_id))
                        chain.append(Comp.Plain(success_msg))
                        yield event.chain_result(chain)

                # 7. 备份
                if (
                    backup_config
                    and backup_config.get("target_sid")
                    and not backup_config.get("only_invalid", False)
                ):
                    await backup_file_to_session(
                        self.context, file_name, backup_config, local_path
                    )

                # 8. 启动延时复核
                asyncio.create_task(
                    self.checker._task_delayed_recheck(
                        event,
                        file_name,
                        file_id,
                        file_component,
                        preview_text,
                        custom_msg_id=target_msg_id,
                        upload_time=upload_time,
                        check_config=check_config,
                        repack_config=repack_config,
                        backup_config=backup_config,
                        local_path=local_path,
                    )
                )

            else:
                # 文件失效，触发通知、补档和备份
                if backup_config and backup_config.get("target_sid"):
                    await backup_file_to_session(
                        self.context, file_name, backup_config, local_path
                    )

                async for msg in self.checker.handle_invalid_file(
                    event,
                    file_name,
                    file_component,
                    preview_text,
                    extra_info,
                    pdf_preview_images,
                    upload_time,
                    check_config,
                    repack_config,
                    local_path=local_path,
                    file_id=file_id,
                ):
                    yield msg

        finally:
            self._remove_temp_path(pdf_preview_file, group_id)
            for image_path in pdf_preview_images:
                self._remove_temp_path(image_path, group_id)

    def _should_download_file(
        self,
        file_name: str,
        file_size: Optional[int],
        preview_config: dict,
        repack_config: dict,
        backup_config: dict,
    ) -> bool:
        """聚合判断是否需要下载文件"""
        return (
            self._needs_preview_download(file_name, file_size, preview_config)
            or self._needs_media_download(file_name, file_size, preview_config)
            or self._needs_repack_download(file_name, repack_config)
            or self._needs_backup_download(file_name, backup_config)
        )

    def _needs_preview_download(
        self, file_name: str, file_size: Optional[int], preview_config: dict
    ) -> bool:
        """判断是否需要为预览下载文件"""
        if self.preview._should_download_for_preview(
            file_name, file_size, preview_config
        ):
            return True
        if (
            self.preview._is_pdf_file(file_name)
            and preview_config.get("pdf_preview_pages", 0) > 0
        ):
            return True
        return False

    def _needs_media_download(
        self, file_name: str, file_size: Optional[int], preview_config: dict
    ) -> bool:
        """判断是否需要为媒体转换下载文件"""
        if self.preview._is_video_file(file_name):
            limit = preview_config.get("auto_convert_video_threshold_mb", 0)
            if limit > 0 and file_size and (file_size / (1024 * 1024)) <= limit:
                return True
        if self.preview._is_image_file(file_name) and preview_config.get(
            "enable_auto_convert_image", False
        ):
            if (
                file_size
                and (file_size / (1024 * 1024)) <= self.image_convert_max_size_mb
            ):
                return True
        return False

    def _needs_repack_download(self, file_name: str, repack_config: dict) -> bool:
        """判断是否需要为补档下载文件"""
        repack_extensions_str = repack_config.get("repack_file_extensions", "").strip()
        if repack_extensions_str:
            repack_file_extensions = [
                ext.strip().lower()
                for ext in repack_extensions_str.split(",")
                if ext.strip()
            ]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip(".")
            return file_ext in repack_file_extensions
        return True  # 未配置后缀限制时，默认需要

    def _needs_backup_download(self, file_name: str, backup_config: dict) -> bool:
        """判断是否需要为备份下载文件"""
        if not backup_config or not backup_config.get("target_sid"):
            return False
        backup_ext_str = backup_config.get("backup_extensions", "").strip()
        if backup_ext_str:
            backup_ext_list = [
                ext.strip().lower() for ext in backup_ext_str.split(",") if ext.strip()
            ]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip(".")
            return file_ext in backup_ext_list
        return True  # 未配置后缀限制，所有文件都备份

    async def _delayed_cleanup_local_path(
        self, local_path: str, delay: int, group_id: int
    ):
        """独立清理任务：等待指定时间后清理预检下载的本地文件"""
        await asyncio.sleep(delay)
        self._remove_temp_path(local_path, group_id)

    async def terminate(self):
        await self.checker._save_cache_to_kv()
        logger.info("QQ 文件预览插件已卸载。")

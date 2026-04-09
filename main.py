import asyncio
import os
from typing import List, Dict, Optional
import time

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp

from .core.utils import get_group_config, is_msg_still_available, react_to_msg, find_file_component, build_notification_text, purify_file_name, backup_file_to_session
from .core.checker import CheckerManager
from .core.preview import PreviewManager

class GroupFileCheckerPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}

        # 全局配置
        global_settings = self.config.get("global_settings", {})
        self.group_whitelist: List[int] = global_settings.get("group_whitelist", [])
        self.group_whitelist = [int(gid) for gid in self.group_whitelist]
        self.file_size_threshold_mb: int = global_settings.get("file_size_threshold_mb", 100)

        # 7za 支持的压缩格式
        self.supported_archive_formats = (
            '.zip', '.7z', '.tar', '.gz', '.bz2', '.xz',
            '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
            '.iso', '.wim', '.rar'
        )

        # 支持文本预览的文件格式
        self.supported_text_formats = (
            '.txt', '.md', '.log',
            '.json', '.xml', '.yaml', '.yml', '.ini', '.conf', '.cfg', '.toml',
            '.py', '.js', '.java', '.c', '.cpp', '.h', '.go', '.rs', '.php', '.rb', '.sh', '.bash',
            '.html', '.htm', '.css', '.jsx', '.tsx', '.ts', '.vue', '.sql',
            '.csv', '.properties', '.env'
        )

        # 图片转换大小限制
        self.image_convert_max_size_mb = 15

        self.temp_dir = os.path.join(StarTools.get_data_dir("astrbot_plugin_file_checker"), "temp")
        os.makedirs(self.temp_dir, exist_ok=True)

        # 文件检查间隔控制
        self.check_interval = 0.3  
        self.last_check_time = None  

        self.download_semaphore = asyncio.Semaphore(5)
        
        # 初始化管理器
        self.checker = CheckerManager(self)
        self.preview = PreviewManager(self)
        
        logger.info("QQ 文件预览插件已加载。")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=2)
    async def on_group_message(self, event: AstrMessageEvent, *args, **kwargs):
        """处理群消息事件"""
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
                if isinstance(segment_dict, dict) and segment_dict.get("type") == "file":
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
                                await event.bot.api.call_action(
                                    "rename_group_file",
                                    group_id=int(group_id),
                                    file_id=file_id,
                                    current_parent_directory=current_parent,
                                    new_name=purified_file_name
                                )
                                logger.info(f"[{group_id}] 文件名已净化并重命名: {original_file_name} -> {purified_file_name}")
                                file_name = purified_file_name  # 使用净化后的文件名继续处理
                            except Exception as e:
                                logger.warning(f"[{group_id}] 文件重命名失败: {e}")
                                # 重命名失败不影响后续流程，使用原文件名继续

                        # 全局大小阈值检查
                        if file_size is not None and self.file_size_threshold_mb > 0:
                            file_size_mb = file_size / (1024 * 1024)
                            if file_size_mb > self.file_size_threshold_mb:
                                logger.debug(f"[{group_id}] 文件 '{file_name}' 超过全局阈值，跳过处理。")
                                return
                        
                        file_component = find_file_component(event)
                        if not file_component:
                            return

                        upload_time = raw_event_data.get("time", int(time.time()))

                        # 重复文件检查逻辑
                        enable_duplicate_check = check_config.get("enable_duplicate_check", False)
                        if enable_duplicate_check and file_size is not None:
                            existing_files = await self.checker._check_if_file_exists_by_size(event, file_name, file_size, upload_time)
                            if existing_files:
                                reply_text = ""
                                if len(existing_files) == 1:
                                    f = existing_files[0]
                                    reply_text = (
                                        f"💡 提醒：您发送的文件「{file_name}」可能与群文件中的「{f.get('file_name')}」重复。\n"
                                        f"  ↳ 上传者: {f.get('uploader_name', '未知')}\n"
                                        f"  ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))}\n"
                                        f"  ↳ 所属文件夹: {f.get('parent_folder_name', '根目录')}"
                                    )
                                else:
                                    reply_text = f"💡 提醒：您发送的文件「{file_name}」可能与群文件中以下 {len(existing_files)} 个文件重复：\n"
                                    for idx, f in enumerate(existing_files, 1):
                                        reply_text += (
                                            f"\n{idx}. {f.get('file_name')}\n"
                                            f"    ↳ 上传者: {f.get('uploader_name', '未知')}\n"
                                            f"    ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))}\n"
                                            f"    ↳ 所属文件夹: {f.get('parent_folder_name', '根目录')}"
                                        )
                                yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)])
                                break

                        # 进入核心检查流程
                        async for result in self._handle_file_check_flow(event, file_name, file_id, file_component, file_size, upload_time, check_config, preview_config, repack_config, backup_config):
                            yield result
                        break
        except Exception as e:
            logger.error(f"处理消息时发生致命错误: {e}", exc_info=True)

    async def _handle_file_check_flow(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, file_size: Optional[int], upload_time: Optional[int], check_config: dict, preview_config: dict, repack_config: dict, backup_config: dict):
        group_id = int(event.get_group_id())
        sender_id = event.get_sender_id()
        self_id = event.get_self_id()
        if sender_id == self_id:
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
        if not await is_msg_still_available(event, event.message_obj.message_id):
            return

        is_valid = await self.checker._check_validity_via_gfs(event, file_id)
        enable_emoji = check_config.get("enable_emoji", True)
        if not is_valid:
            await asyncio.sleep(1)
            retry_valid = await self.checker._check_validity_via_gfs(event, file_id)
            is_valid = retry_valid

        # 统一文件生命周期管理：集中判断、统一下载、统一清理
        local_path: Optional[str] = None
        try:
            # 1. 聚合判断是否需要下载文件
            needs_download = self._should_download_file(
                file_name, file_size, preview_config, repack_config, backup_config
            )

            # 2. 统一下载：先由框架下载到临时位置，再复制到插件 temp_dir
            if needs_download:
                async with self.download_semaphore:
                    framework_temp_path = await file_component.get_file()
                # 复制到插件自己的 temp_dir，使用原始文件名，后续所有操作都针对此副本
                import shutil
                local_path = os.path.join(self.temp_dir, file_name)
                # 如果已存在，覆盖
                if os.path.exists(local_path):
                    os.remove(local_path)
                shutil.copy2(framework_temp_path, local_path)
                logger.debug(f"[{group_id}] 文件已复制到插件 temp_dir: {local_path}")

            # 3. 预览生成
            preview_text, extra_info = await self.preview._get_preview_for_file(
                file_name, local_path, file_size, preview_config
            )

            # 4. PDF 预览图生成
            pdf_preview_images = []
            if preview_text.startswith('PDF_PATH:'):
                # 压缩包内返回的 PDF 路径，需要清理
                pdf_preview_file = preview_text[9:]  # 去掉 'PDF_PATH:' 前缀
                preview_text = ""  # 清空预览文本
                if pdf_preview_file and os.path.exists(pdf_preview_file) and preview_config.get("pdf_preview_pages", 0) > 0:
                    try:
                        pdf_preview_images = await self.preview._get_pdf_preview(pdf_preview_file, preview_config)
                    except Exception as e:
                        logger.error(f"PDF预览处理出错: {e}", exc_info=True)
                    # 生成预览图后立即清理 PDF 临时文件
                    try:
                        os.remove(pdf_preview_file)
                        logger.debug(f"[{group_id}] 🗑️ 已清理压缩包内 PDF 临时文件: {pdf_preview_file}")
                    except OSError as e:
                        logger.warning(f"[{group_id}] ⚠️ 删除 PDF 临时文件失败: {e}")
            elif self.preview._is_pdf_file(file_name) and preview_config.get("pdf_preview_pages", 0) > 0 and local_path:
                # 外层 PDF 文件（local_path 已下载）
                try:
                    pdf_preview_images = await self.preview._get_pdf_preview(local_path, preview_config)
                except Exception as e:
                    logger.error(f"PDF预览处理出错: {e}", exc_info=True)

            if is_valid:
                has_any_preview = bool(preview_text or pdf_preview_images)
                await react_to_msg(event, "314" if has_any_preview else "320", enable_emoji)

                # 5. 自动转换媒体
                if self.preview._is_video_file(file_name):
                    limit = preview_config.get("auto_convert_video_threshold_mb", 0)
                    if limit > 0 and file_size and (file_size / (1024*1024)) <= limit:
                        async for r in self.preview._convert_file_to_media(
                            event, file_name, file_size, local_path, "video"
                        ):
                            yield r

                if self.preview._is_image_file(file_name) and preview_config.get("enable_auto_convert_image", False):
                    if file_size and (file_size / (1024*1024)) <= self.image_convert_max_size_mb:
                        async for r in self.preview._convert_file_to_media(
                            event, file_name, file_size, local_path, "image"
                        ):
                            yield r

                # 6. 发送通知文案
                if check_config.get("notify_on_success", True) or has_any_preview:
                    success_msg = build_notification_text(
                        file_name, True, preview_text, extra_info, preview_config
                    )
                    if pdf_preview_images:
                        for msg in self.preview.send_pdf_preview(event, success_msg, pdf_preview_images):
                            yield msg
                    else:
                        chain = [Comp.Reply(id=event.message_obj.message_id)]
                        chain.append(Comp.Plain(success_msg))
                        yield event.chain_result(chain)

                # 7. 备份
                if backup_config and backup_config.get("target_sid") and not backup_config.get("only_invalid", False):
                    await backup_file_to_session(self.context, file_name, backup_config, local_path)

                # 8. 启动延时复核
                asyncio.create_task(self.checker._task_delayed_recheck(
                    event, file_name, file_id, file_component, preview_text,
                    upload_time=upload_time, check_config=check_config, repack_config=repack_config,
                    backup_config=backup_config, local_path=local_path
                ))

            else:
                # 文件失效，触发通知、补档和备份
                if backup_config and backup_config.get("target_sid"):
                    await backup_file_to_session(self.context, file_name, backup_config, local_path)

                async for msg in self.checker.handle_invalid_file(
                    event, file_name, file_component, preview_text, extra_info,
                    pdf_preview_images, upload_time, check_config, repack_config, local_path=local_path
                ):
                    yield msg

        finally:
            cleanup_delay = check_config.get("check_delay_seconds", 300) * 2
            if local_path:
                asyncio.create_task(self._delayed_cleanup_local_path(local_path, cleanup_delay, group_id))

    def _should_download_file(
        self,
        file_name: str,
        file_size: Optional[int],
        preview_config: dict,
        repack_config: dict,
        backup_config: dict
    ) -> bool:
        """聚合判断是否需要下载文件"""
        return (
            self._needs_preview_download(file_name, file_size, preview_config) or
            self._needs_media_download(file_name, file_size, preview_config) or
            self._needs_repack_download(file_name, repack_config) or
            self._needs_backup_download(file_name, backup_config)
        )

    def _needs_preview_download(self, file_name: str, file_size: Optional[int], preview_config: dict) -> bool:
        """判断是否需要为预览下载文件"""
        if self.preview._should_download_for_preview(file_name, file_size, preview_config):
            return True
        if self.preview._is_pdf_file(file_name) and preview_config.get("pdf_preview_pages", 0) > 0:
            return True
        return False

    def _needs_media_download(self, file_name: str, file_size: Optional[int], preview_config: dict) -> bool:
        """判断是否需要为媒体转换下载文件"""
        if self.preview._is_video_file(file_name):
            limit = preview_config.get("auto_convert_video_threshold_mb", 0)
            if limit > 0 and file_size and (file_size / (1024*1024)) <= limit:
                return True
        if self.preview._is_image_file(file_name) and preview_config.get("enable_auto_convert_image", False):
            if file_size and (file_size / (1024*1024)) <= self.image_convert_max_size_mb:
                return True
        return False

    def _needs_repack_download(self, file_name: str, repack_config: dict) -> bool:
        """判断是否需要为补档下载文件"""
        repack_extensions_str = repack_config.get("repack_file_extensions", "").strip()
        if repack_extensions_str:
            repack_file_extensions = [ext.strip().lower() for ext in repack_extensions_str.split(",") if ext.strip()]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
            return file_ext in repack_file_extensions
        return True  # 未配置后缀限制时，默认需要

    def _needs_backup_download(self, file_name: str, backup_config: dict) -> bool:
        """判断是否需要为备份下载文件"""
        if not backup_config or not backup_config.get("target_sid"):
            return False
        backup_ext_str = backup_config.get("backup_extensions", "").strip()
        if backup_ext_str:
            backup_ext_list = [ext.strip().lower() for ext in backup_ext_str.split(",") if ext.strip()]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
            return file_ext in backup_ext_list
        return True  # 未配置后缀限制，所有文件都备份

    async def _delayed_cleanup_local_path(self, local_path: str, delay: int, group_id: int):
        """独立清理任务：等待指定时间后清理预检下载的本地文件"""
        await asyncio.sleep(delay)
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.debug(f"[{group_id}] 🗑️ 已清理预检下载的本地文件: {local_path}")
            except OSError as e:
                logger.warning(f"[{group_id}] ⚠️ 删除临时文件失败: {e}")

    async def terminate(self):
        logger.info("QQ 文件预览插件已卸载。")

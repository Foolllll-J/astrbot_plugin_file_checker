import os
import asyncio
import time
import subprocess
import re
import uuid
import shutil
from typing import List, Dict, Optional
from urllib.parse import urlsplit, urlunsplit

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from .utils import (
    is_msg_still_available,
    react_to_msg,
    get_group_config,
    build_notification_text,
    backup_file_to_session,
    is_action_success,
    format_seconds,
)


DUPLICATE_TIMESTAMP_TOLERANCE_SECONDS = 20


def _mask_url_for_log(url: str) -> str:
    if not url:
        return url

    try:
        parts = urlsplit(url)
        masked_query = "<masked>" if parts.query else ""
        masked_fragment = "<masked>" if parts.fragment else ""
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, masked_query, masked_fragment)
        )
    except Exception:
        return "<unparseable-url>"


def _is_known_invalid_file_error(error: Exception) -> bool:
    error_text = str(error)
    return "文件下载失败（-134）" in error_text or "code=-134" in error_text


def _normalize_action_payload(result):
    if isinstance(result, dict) and isinstance(result.get("data"), dict):
        return result.get("data", {})
    return result if isinstance(result, dict) else {}


def _is_ob11_action_success(result) -> bool:
    return (
        isinstance(result, dict)
        and result.get("status") == "ok"
        and result.get("retcode") == 0
    )


def _is_llbot_action_success(result) -> bool:
    return is_action_success(result)


def _needs_legacy_file_count(backend_type) -> bool:
    """NapCat/未知协议端需要旧版 file_count，LLBot 和 SnowLuma 不需要。"""
    if isinstance(backend_type, bool):
        return not backend_type
    return str(backend_type).strip().lower() not in {"llbot", "snowluma"}


def _get_plugin_backend_type(plugin) -> str:
    """读取新后端类型，并兼容旧调用方提供的 _is_llbot 字段。"""
    backend_type = getattr(plugin, "_backend_type", None)
    if backend_type:
        return backend_type
    return "llbot" if getattr(plugin, "_is_llbot", False) else "napcat"


class CheckerManager:
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self.temp_dir = plugin_instance.temp_dir
        self.download_semaphore = plugin_instance.download_semaphore
        self.supported_archive_formats = plugin_instance.supported_archive_formats
        self.supported_text_formats = plugin_instance.supported_text_formats

        # 文件索引缓存: group_id -> dict (纯内存，不持久化)
        self._cache: Dict[str, dict] = {}
        self._cache_lock = asyncio.Lock()

    async def _delete_group_file(
        self, event: AstrMessageEvent, file_id: str, file_name: str
    ) -> bool:
        """删除群文件"""
        group_id = int(event.get_group_id())
        try:
            client = event.bot
            delete_result = await client.api.call_action(
                "delete_group_file", group_id=group_id, file_id=file_id
            )
            if is_action_success(delete_result):
                logger.info(f"[{group_id}] ✅ 成功删除群文件: {file_name}")
                return True
            logger.warning(
                f"[{group_id}] ⚠️ 删除群文件失败: {file_name}, 返回={delete_result}"
            )
            return False
        except Exception as e:
            logger.error(f"[{group_id}] ❌ 删除群文件时发生错误: {e}", exc_info=True)
            return False

    async def _delete_group_file_list(
        self, event: AstrMessageEvent, file_list: list[dict]
    ):
        for f in file_list:
            fid = f.get("file_id")
            fname = f.get("file_name", "未知")
            if fid:
                await self._delete_group_file(event, fid, fname)

    async def _search_file_id_by_name(
        self,
        event: AstrMessageEvent,
        file_name: str,
        target_time: Optional[int] = None,
    ) -> Optional[str]:
        group_id = int(event.get_group_id())
        gid = str(group_id)

        # 确保缓存存在且新鲜
        async with self._cache_lock:
            if gid not in self._cache:
                logger.info(f"[{gid}] 搜索文件时缓存缺失，触发全量扫描")
                self._cache[gid] = await self._do_full_scan(event)
            else:
                await self._ensure_cache_fresh(event)

        # 缓存查询
        cached_group = self._cache[gid]
        matched = [
            (fid, info)
            for fid, info in cached_group.get("flat_index", {}).items()
            if info.get("file_name") == file_name
        ]
        if not matched:
            logger.debug(f"[{gid}] 缓存中未找到文件 '{file_name}'")
            return None

        if len(matched) == 1 or target_time is None:
            fid = matched[0][0]
            logger.debug(f"[{gid}] 缓存命中，文件 '{file_name}' 的 file_id: {fid}")
            return fid
        closest = min(
            matched,
            key=lambda item: abs(item[1].get("modify_time", 0) - target_time),
        )
        fid = closest[0]
        logger.debug(
            f"[{gid}] 缓存命中，多个同名文件，按时间匹配到 '{file_name}' 的 file_id: {fid}"
        )
        return fid

    @staticmethod
    def _get_folder_modify_time(folder_info: dict, backend_type: str = "") -> int:
        """获取文件夹级变更时间；SnowLuma 用 last_upload_time 表示该字段。"""
        modify_time = folder_info.get("modify_time")
        if modify_time:
            return modify_time
        if str(backend_type).strip().lower() == "snowluma":
            return folder_info.get("last_upload_time", 0) or 0
        return 0

    def _detect_modify_time_support(
        self, payload: dict, backend_type: str = ""
    ) -> bool:
        folders = payload.get("folders", [])
        return any(
            self._get_folder_modify_time(folder, backend_type) > 0
            for folder in folders
        )

    def _calc_root_modify_time(self, payload: dict, backend_type: str = "") -> int:
        mt = 0
        for f in payload.get("files", []):
            mt = max(mt, f.get("modify_time", 0) or 0)
        for fol in payload.get("folders", []):
            mt = max(mt, self._get_folder_modify_time(fol, backend_type))
        return mt

    async def _do_full_scan_group(self, group_id: int, client, backend_type: str) -> dict:
        """全量扫描所有文件夹，返回完整的缓存数据（client 版本，供外部/内部共用）"""
        gid = str(group_id)

        logger.info(f"[{gid}] [全量扫描] 开始扫描所有文件夹")

        flat_index = {}
        root_files_sig = {}
        root_folder_counts = {}
        folder_name_map = {"/": "根目录"}
        folders_info = {}
        folders_to_scan = [{"folder_id": "/", "folder_name": "根目录"}]
        has_modify_time = False
        root_modify_time = 0
        first_root_result = None
        total_folders_scanned = 0

        while folders_to_scan:
            cur = folders_to_scan.pop(0)
            cur_id = cur["folder_id"]
            cur_name = cur["folder_name"]
            total_folders_scanned += 1

            try:
                if cur_id == "/":
                    result = await client.api.call_action(
                        "get_group_root_files", group_id=group_id
                    )
                    if first_root_result is None:
                        first_root_result = result
                else:
                    if _needs_legacy_file_count(backend_type):
                        result = await client.api.call_action(
                            "get_group_files_by_folder",
                            group_id=group_id,
                            folder_id=cur_id,
                            file_count=1000,
                        )
                    else:
                        result = await client.api.call_action(
                            "get_group_files_by_folder",
                            group_id=group_id,
                            folder_id=cur_id,
                        )

                payload = _normalize_action_payload(result)
                if not isinstance(payload, dict):
                    logger.warning(
                        f"[{gid}] 全量扫描: 文件夹 '{cur_name}' API返回了意料之外的格式。"
                    )
                    continue

                if cur_id == "/":
                    has_modify_time = self._detect_modify_time_support(
                        payload, backend_type
                    )
                    root_modify_time = self._calc_root_modify_time(
                        payload, backend_type
                    )

                for file_info in payload.get("files", []):
                    fid = file_info.get("file_id")
                    if not fid:
                        continue
                    fname = file_info.get("file_name", "")
                    rpath = (
                        os.path.join(cur_name, fname) if cur_name != "根目录" else fname
                    )
                    flat_index[fid] = {
                        "file_id": fid,
                        "file_name": fname,
                        "file_size": file_info.get("file_size"),
                        "modify_time": file_info.get("modify_time"),
                        "busid": file_info.get("busid"),
                        "uploader_name": file_info.get("uploader_name"),
                        "parent_folder_id": cur_id,
                        "parent_folder_name": cur_name,
                        "relative_path": rpath,
                    }
                    if cur_id == "/":
                        root_files_sig[fid] = file_info.get("modify_time")

                for folder_info in payload.get("folders", []):
                    sub_id = folder_info.get("folder_id")
                    if not sub_id:
                        continue
                    folders_to_scan.append(
                        {
                            "folder_id": sub_id,
                            "folder_name": folder_info.get("folder_name"),
                        }
                    )
                    folder_name_map[sub_id] = folder_info.get("folder_name")
                    if cur_id == "/":
                        root_folder_counts[sub_id] = folder_info.get(
                            "total_file_count", 0
                        )
                        folders_info[sub_id] = {
                            "folder_name": folder_info.get("folder_name"),
                            "modify_time": self._get_folder_modify_time(
                                folder_info, backend_type
                            ),
                            "total_file_count": folder_info.get("total_file_count", 0),
                        }

            except Exception as e:
                logger.error(
                    f"[{gid}] 全量扫描遍历文件夹 '{cur_name}' 时出错: {e}",
                    exc_info=True,
                )

        result = {
            "has_modify_time": has_modify_time,
            "root_modify_time": root_modify_time,
            "root_file_count": len(root_files_sig),
            "folders": folders_info,
            "flat_index": flat_index,
            "root_files_sig": root_files_sig,
            "root_folder_counts": root_folder_counts,
            "folder_name_map": folder_name_map,
        }

        logger.info(
            f"[{gid}] [全量扫描] 完成: "
            f"扫描了 {total_folders_scanned} 个文件夹, "
            f"共 {len(flat_index)} 个文件"
        )

        return result

    async def _do_full_scan(self, event: AstrMessageEvent) -> dict:
        """全量扫描（event 版本，委派给 _do_full_scan_group）"""
        return await self._do_full_scan_group(
            int(event.get_group_id()), event.bot, _get_plugin_backend_type(self.plugin)
        )

    async def _sync_single_folder(
        self, group_id: int, folder_id: str, call_action, backend_type: str, cached: dict
    ):
        """增量刷新单个文件夹的缓存"""
        gid = str(group_id)
        logger.debug(f"[{gid}] [缓存刷新] 增量刷新文件夹 id={folder_id}")
        flat_index = cached.get("flat_index", {})
        root_files_sig = cached.get("root_files_sig", {})
        root_folder_counts = cached.get("root_folder_counts", {})
        folder_name_map = cached.get("folder_name_map", {})
        try:
            if folder_id == "/":
                result = await call_action("get_group_root_files", group_id=group_id)
            else:
                if _needs_legacy_file_count(backend_type):
                    result = await call_action(
                        "get_group_files_by_folder",
                        group_id=group_id,
                        folder_id=folder_id,
                        file_count=1000,
                    )
                else:
                    result = await call_action(
                        "get_group_files_by_folder",
                        group_id=group_id,
                        folder_id=folder_id,
                    )

            payload = _normalize_action_payload(result)
            if not isinstance(payload, dict):
                logger.warning(
                    f"[{gid}] 增量刷新文件夹 {folder_id}: API返回了意料之外的格式。"
                )
                return

            current_folder_name = folder_name_map.get(folder_id, folder_id)
            current_file_ids = set()
            added = 0
            removed = 0

            for file_info in payload.get("files", []):
                fid = file_info.get("file_id")
                if not fid:
                    continue
                current_file_ids.add(fid)
                was_new = fid not in flat_index
                fname = file_info.get("file_name", "")
                rpath = (
                    os.path.join(current_folder_name, fname)
                    if current_folder_name != "根目录"
                    else fname
                )
                flat_index[fid] = {
                    "file_id": fid,
                    "file_name": fname,
                    "file_size": file_info.get("file_size"),
                    "modify_time": file_info.get("modify_time"),
                    "busid": file_info.get("busid"),
                    "uploader_name": file_info.get("uploader_name"),
                    "parent_folder_id": folder_id,
                    "parent_folder_name": current_folder_name,
                    "relative_path": rpath,
                }
                if was_new:
                    added += 1
                if folder_id == "/":
                    root_files_sig[fid] = file_info.get("modify_time")

            existing_ids = list(flat_index.keys())
            for fid in existing_ids:
                if (
                    flat_index[fid].get("parent_folder_id") == folder_id
                    and fid not in current_file_ids
                ):
                    flat_index.pop(fid, None)
                    removed += 1
                    if folder_id == "/":
                        root_files_sig.pop(fid, None)

            logger.debug(
                f"[{gid}] [增量刷新] 文件夹 '{current_folder_name}' 完成: "
                f"新增 {added} 个, 移除 {removed} 个, "
                f"当前共 {len(current_file_ids)} 个文件"
            )

            if folder_id == "/":
                # 更新根目录下的子文件夹信息
                for folder_info in payload.get("folders", []):
                    sub_id = folder_info.get("folder_id")
                    if sub_id:
                        root_folder_counts[sub_id] = folder_info.get(
                            "total_file_count", 0
                        )
                        folder_name_map[sub_id] = folder_info.get("folder_name")
                        if "folders" in cached:
                            cached["folders"][sub_id] = {
                                "folder_name": folder_info.get("folder_name"),
                                "modify_time": self._get_folder_modify_time(
                                    folder_info, backend_type
                                ),
                                "total_file_count": folder_info.get(
                                    "total_file_count", 0
                                ),
                            }

            cached["flat_index"] = flat_index
            cached["root_files_sig"] = root_files_sig
            cached["root_folder_counts"] = root_folder_counts
            cached["folder_name_map"] = folder_name_map

        except Exception as e:
            logger.error(
                f"[{gid}] 增量刷新文件夹 '{current_folder_name}' 时出错: {e}",
                exc_info=True,
            )

    async def _ensure_cache_fresh_group(
        self, group_id: int, client, backend_type: str
    ) -> tuple:
        """
        确保缓存新鲜（client 版本，供外部/内部共用）。
        缓存不存在 → 返回 (None, False)
        缓存存在 → 1 次 get_group_root_files 对比签名，按需增量刷新。
        返回 (flat_index_or_None, changed: bool)
        """
        gid = str(group_id)

        if gid not in self._cache:
            return None, False

        cached = self._cache[gid]

        try:

            async def call_action_fn(name: str, **kw):
                return await client.api.call_action(name, **kw)

            result = await call_action_fn("get_group_root_files", group_id=group_id)
            payload = _normalize_action_payload(result)
            if not isinstance(payload, dict):
                return cached.get("flat_index", {}), False

            has_mt = self._detect_modify_time_support(payload, backend_type)

            if has_mt:
                return await self._ensure_fresh_mt(
                    cached, payload, group_id, call_action_fn, backend_type
                )
            else:
                return await self._ensure_fresh_fallback(
                    cached, payload, group_id, call_action_fn, backend_type
                )

        except Exception as e:
            logger.error(f"[{gid}] 缓存新鲜度检查出错: {e}", exc_info=True)

        return cached.get("flat_index", {}), False

    async def _ensure_cache_fresh(self, event: AstrMessageEvent):
        """确保缓存新鲜（event 版本，委派给 _ensure_cache_fresh_group）"""
        return await self._ensure_cache_fresh_group(
            int(event.get_group_id()), event.bot, _get_plugin_backend_type(self.plugin)
        )

    async def _ensure_fresh_mt(
        self,
        cached: dict,
        payload: dict,
        int_group_id: int,
        call_action_fn,
        backend_type: str = "napcat",
    ) -> tuple:
        """modify_time 模式的缓存验证"""
        group_id = str(int_group_id)

        new_root_mt = self._calc_root_modify_time(payload, backend_type)

        # 更新 root_files_sig 和 flat_index 中的根目录文件
        old_sig = cached.get("root_files_sig", {})
        new_sig = {}
        for f in payload.get("files", []):
            fid = f.get("file_id")
            if fid:
                new_sig[fid] = f.get("modify_time")
                fname = f.get("file_name", "")
                cached["flat_index"][fid] = {
                    "file_id": fid,
                    "file_name": fname,
                    "file_size": f.get("file_size"),
                    "modify_time": f.get("modify_time"),
                    "busid": f.get("busid"),
                    "uploader_name": f.get("uploader_name"),
                    "parent_folder_id": "/",
                    "parent_folder_name": "根目录",
                    "relative_path": fname,
                }

        # 移除根目录已删除的文件
        for fid in list(old_sig.keys()):
            if fid not in new_sig:
                cached["flat_index"].pop(fid, None)

        cached["root_files_sig"] = new_sig

        # 比对文件夹信息
        old_folders = cached.get("folders", {})
        new_folders = {}
        folder_name_map = cached.get("folder_name_map", {"/": "根目录"})
        changed = new_root_mt != cached.get("root_modify_time", 0)

        for fol in payload.get("folders", []):
            fid = fol.get("folder_id")
            if not fid:
                continue
            folder_mt = self._get_folder_modify_time(fol, backend_type)
            fcount = fol.get("total_file_count", 0)
            fname = fol.get("folder_name")
            new_folders[fid] = {
                "folder_name": fname,
                "modify_time": folder_mt,
                "total_file_count": fcount,
            }
            folder_name_map[fid] = fname

            old_entry = old_folders.get(fid)
            if (
                old_entry
                and old_entry["modify_time"] == folder_mt
                and old_entry["total_file_count"] == fcount
            ):
                continue
            logger.debug(f"[{group_id}] 文件夹 {fname} 有变更，增量刷新")
            await self._sync_single_folder(
                int_group_id, fid, call_action_fn, backend_type, cached
            )
            changed = True

        # 移除已删除的文件夹及其文件
        for fid in list(old_folders.keys()):
            if fid not in new_folders:
                fname = folder_name_map.pop(fid, fid)
                logger.debug(f"[{group_id}] 文件夹 {fname} 已删除")
                for file_id, info in list(cached["flat_index"].items()):
                    if info.get("parent_folder_id") == fid:
                        cached["flat_index"].pop(file_id, None)
                changed = True

        cached["folders"] = new_folders
        cached["root_modify_time"] = new_root_mt
        cached["root_file_count"] = len(new_sig)
        cached["folder_name_map"] = folder_name_map

        logger.debug(
            f"[{group_id}] [缓存刷新] mt模式完成: changed={changed}, 文件数={len(cached.get('flat_index', {}))}"
        )
        return cached.get("flat_index", {}), changed

    async def _ensure_fresh_fallback(
        self,
        cached: dict,
        payload: dict,
        int_group_id: int,
        call_action_fn,
        backend_type: str = "napcat",
    ) -> tuple:
        """不支持 modify_time 的降级方案：沿用旧的 count + 签名对比"""
        group_id = str(int_group_id)
        changed = False

        # 比对根目录文件签名
        old_sig = cached.get("root_files_sig", {})
        new_sig = {}
        for f in payload.get("files", []):
            fid = f.get("file_id")
            if fid:
                new_sig[fid] = f.get("modify_time")

        for f in payload.get("files", []):
            fid = f.get("file_id")
            if not fid:
                continue
            fname = f.get("file_name", "")
            cached["flat_index"][fid] = {
                "file_id": fid,
                "file_name": fname,
                "file_size": f.get("file_size"),
                "modify_time": f.get("modify_time"),
                "busid": f.get("busid"),
                "uploader_name": f.get("uploader_name"),
                "parent_folder_id": "/",
                "parent_folder_name": "根目录",
                "relative_path": fname,
            }
            if old_sig.get(fid) != new_sig.get(fid):
                changed = True

        for fid in list(old_sig.keys()):
            if fid not in new_sig:
                cached["flat_index"].pop(fid, None)
                changed = True

        cached["root_files_sig"] = new_sig

        # 比对子文件夹
        old_fc = cached.get("root_folder_counts", {})
        folder_name_map = cached.get("folder_name_map", {"/": "根目录"})

        current_folder_ids = set()
        new_fc = {}
        for fol in payload.get("folders", []):
            fid = fol.get("folder_id")
            if fid:
                current_folder_ids.add(fid)
                new_fc[fid] = fol.get("total_file_count", 0)

        for fid in current_folder_ids:
            if old_fc.get(fid) != new_fc.get(fid):
                logger.debug(
                    f"[{group_id}] 文件夹 {folder_name_map.get(fid, fid)} count 变化，增量刷新"
                )
                await self._sync_single_folder(
                    int_group_id, fid, call_action_fn, backend_type, cached
                )
                changed = True

        for fid in list(old_fc.keys()):
            if fid not in current_folder_ids:
                fname = folder_name_map.pop(fid, fid)
                logger.debug(f"[{group_id}] 文件夹 {fname} 已删除")
                for file_id, info in list(cached["flat_index"].items()):
                    if info.get("parent_folder_id") == fid:
                        cached["flat_index"].pop(file_id, None)
                changed = True

        for fid in current_folder_ids:
            if fid not in old_fc:
                logger.debug(
                    f"[{group_id}] 发现新文件夹 {folder_name_map.get(fid, fid)}，增量扫描"
                )
                await self._sync_single_folder(
                    int_group_id, fid, call_action_fn, backend_type, cached
                )
                changed = True

        cached["root_folder_counts"] = new_fc

        for fol in payload.get("folders", []):
            fid = fol.get("folder_id")
            if fid:
                folder_name_map[fid] = fol.get("folder_name")

        logger.debug(
            f"[{group_id}] [缓存刷新] fallback完成: changed={changed}, 文件数={len(cached.get('flat_index', {}))}"
        )
        return cached.get("flat_index", {}), changed

    def _find_duplicates_in_flat_index(
        self,
        flat_index: dict,
        file_size: int,
        upload_time: int,
        enable_self_exclude: bool = True,
    ) -> List[Dict]:
        # 当前方法被调用时 caller 已携带 group_id 上下文，这里从 caller 传递日志上下文
        possible_duplicates = []
        for info in flat_index.values():
            if info.get("file_size") == file_size:
                possible_duplicates.append(info)

        closest = min(
            (f for f in possible_duplicates if f.get("modify_time") is not None),
            key=lambda f: abs(f.get("modify_time") - upload_time),
            default=None,
        )

        existing_files = []
        for f in possible_duplicates:
            mtime = f.get("modify_time")
            if (
                enable_self_exclude
                and mtime is not None
                and f is closest
                and abs(mtime - upload_time) <= DUPLICATE_TIMESTAMP_TOLERANCE_SECONDS
            ):
                continue
            existing_files.append(f)

        return existing_files

    def _check_old_cache_duplicates(
        self, group_id_str: str, file_size: int, upload_time: int
    ) -> List[Dict]:
        cached = self._cache.get(group_id_str)
        if not cached:
            return []
        flat = cached.get("flat_index", {})
        return self._find_duplicates_in_flat_index(
            flat, file_size, upload_time, enable_self_exclude=False
        )

    async def _get_duplicate_confirmed(
        self,
        event: AstrMessageEvent,
        group_id_str: str,
        file_size: Optional[int],
        upload_time: int,
    ) -> List[Dict]:
        """Build/refresh cache and return confirmed duplicates.
        Cache is guaranteed fresh after this call. 0 API if hot and no match.
        """
        gid = str(group_id_str)

        async with self._cache_lock:
            is_cold = gid not in self._cache
            if is_cold:
                if await self._load_cache_from_kv_for_group(gid):
                    is_cold = False
        if is_cold:
            logger.info(f"[{gid}] 缓存缺失，触发全量扫描")
            scan_result = await self._do_full_scan(event)
            async with self._cache_lock:
                if gid not in self._cache:
                    self._cache[gid] = scan_result

        if file_size is None:
            if not is_cold:
                async with self._cache_lock:
                    await self._ensure_cache_fresh(event)
            return []

        suspected = []
        if not is_cold:
            suspected = self._check_old_cache_duplicates(gid, file_size, upload_time)
            logger.debug(f"[{gid}] [查重] 旧缓存查到 {len(suspected)} 个嫌疑项")

        if not is_cold:
            async with self._cache_lock:
                fresh_flat, changed = await self._ensure_cache_fresh(event)
                if not changed:
                    cached = self._cache.get(gid, {})
                    if not cached.get("has_modify_time"):
                        logger.info(f"[{gid}] fallback 刷新未检测到变更，执行全量扫描")
                        self._cache[gid] = await self._do_full_scan(event)
                        fresh_flat = self._cache[gid].get("flat_index", {})
        else:
            fresh_flat = self._cache[gid].get("flat_index", {})

        if is_cold or suspected:
            result = self._find_duplicates_in_flat_index(
                fresh_flat, file_size, upload_time
            )
            logger.debug(
                f"[{gid}] [查重] 发现 {len(result)} 个重复文件 "
                f"({[f.get('file_name') for f in result]})"
            )
            return result
        return []

    async def _repack_and_send_file(
        self,
        event: AstrMessageEvent,
        original_filename: str,
        file_component: Comp.File,
        repack_config: dict = None,
        local_path: Optional[str] = None,
    ):
        """重新打包失效文件（直接使用 local_path）"""
        if repack_config is None:
            repack_config = {}
        repack_zip_password = repack_config.get("repack_zip_password", "")

        original_file_name = os.path.basename(original_filename)
        if re.search(r'[\\/|*<>;"\x00-\x1F\x7F]', original_file_name):
            logger.error(
                f"文件名 '{original_filename}' 包含非安全字符，已跳过重新打包。"
            )
            yield event.chain_result(
                [
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.Plain("❌ 文件名包含不安全字符，已跳过重新打包。"),
                ]
            )
            return

        if local_path is None or not os.path.exists(local_path):
            logger.warning(f"无可用本地文件，跳过重新打包: {original_filename}")
            yield event.chain_result(
                [
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.Plain("❌ 无可用文件，无法重新打包。"),
                ]
            )
            return

        repacked_file_path = None
        repack_source_dir = None
        try:
            logger.debug(f"开始为失效文件 {original_filename} 进行重新打包...")

            base_name = os.path.splitext(original_filename)[0]
            new_zip_name = f"{base_name}.zip"
            repacked_file_path = os.path.join(
                self.temp_dir, f"{uuid.uuid4().hex}_{new_zip_name}"
            )

            # 独立临时目录并恢复原文件名，避免 ZIP 内部条目使用临时文件名。
            repack_source_dir = os.path.join(
                self.temp_dir, f"repack_source_{uuid.uuid4().hex}"
            )
            os.makedirs(repack_source_dir, exist_ok=False)
            repack_source_path = os.path.join(repack_source_dir, original_file_name)
            shutil.copy2(local_path, repack_source_path)

            command = ["zip", "-j", repacked_file_path, repack_source_path]
            if repack_zip_password:
                command.extend(["-P", repack_zip_password])

            logger.debug(f"正在执行打包命令: {' '.join(command)}")
            process = await asyncio.create_subprocess_exec(
                *command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_message = stderr.decode("utf-8")
                logger.error(f"使用 zip 命令打包文件时出错: {error_message}")
                yield event.chain_result(
                    [
                        Comp.Reply(id=event.message_obj.message_id),
                        Comp.Plain(f"❌ 重新打包失败，错误信息：\n{error_message}"),
                    ]
                )
                return

            logger.debug(f"文件已重新打包至 {repacked_file_path}，准备发送...")

            reply_text = "已重新打包为 ZIP 文件"
            if repack_zip_password:
                reply_text += f"（密码：{repack_zip_password}）"
            reply_text += "发送："
            file_component_to_send = Comp.File(
                file=repacked_file_path, name=new_zip_name
            )

            new_msg_id = None

            # 补档通知使用框架的消息发送方式
            yield event.chain_result(
                [Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)]
            )

            # 标记为补档文件，防止发送后钩子循环处理
            event.set_extra("_is_repack_file", True)

            # 发送文件并获取文件消息的 ID
            if isinstance(event, AiocqhttpMessageEvent):
                try:
                    # 使用 call_action 发送文件以获取 message_id
                    absolute_path = os.path.abspath(repacked_file_path)
                    file_msg = (
                        f"[CQ:file,file=file:///{absolute_path},name={new_zip_name}]"
                    )
                    ret = await event.bot.api.call_action(
                        "send_group_msg",
                        group_id=int(event.get_group_id()),
                        message=file_msg,
                    )
                    if isinstance(ret, dict):
                        new_msg_id = str(
                            ret.get("message_id")
                            or ret.get("data", {}).get("message_id")
                            or ""
                        )
                except Exception as e:
                    logger.warning(f"尝试获取补档文件消息ID失败: {e}")
                    yield event.chain_result([file_component_to_send])
            else:
                yield event.chain_result([file_component_to_send])

            await asyncio.sleep(2)

            new_file_id = await self._search_file_id_by_name(event, new_zip_name)

            if new_file_id:
                logger.info(f"新文件发送成功，ID为 {new_file_id}，已加入延时复核队列。")
                # 将获取到的通知消息 ID 传递给复核任务
                check_config = get_group_config(
                    self.plugin.config, str(event.get_group_id()), "check_module"
                )
                asyncio.create_task(
                    self._task_delayed_recheck(
                        event,
                        new_zip_name,
                        new_file_id,
                        None,
                        None,
                        custom_msg_id=new_msg_id,
                        upload_time=int(time.time()),
                        check_config=check_config,
                        repack_config=repack_config,
                    )
                )
            else:
                logger.error("未能获取新文件的ID，无法进行延时复核。")

        except FileNotFoundError:
            logger.error("重新打包失败：容器内未找到 zip 命令。请安装 zip。")
            yield event.chain_result(
                [
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.Plain(
                        "❌ 重新打包失败。容器内未找到 zip 命令，请联系管理员安装。"
                    ),
                ]
            )
        except Exception as e:
            logger.error(f"重新打包并发送文件时出错: {e}", exc_info=True)
            yield event.chain_result(
                [
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.Plain("❌ 重新打包并发送文件失败。"),
                ]
            )
        finally:
            if repack_source_dir and os.path.exists(repack_source_dir):
                shutil.rmtree(repack_source_dir, ignore_errors=True)
            if repacked_file_path and os.path.exists(repacked_file_path):

                async def cleanup_file(path: str):
                    await asyncio.sleep(10)
                    try:
                        os.remove(path)
                        logger.debug(f"已清理临时文件: {path}")
                    except OSError as e:
                        logger.warning(f"删除临时文件 {path} 失败: {e}")

                asyncio.create_task(cleanup_file(repacked_file_path))

    async def _check_validity_via_gfs(
        self, event: AstrMessageEvent, file_id: str
    ) -> bool:
        """检查文件有效性"""
        group_id = int(event.get_group_id())
        try:
            assert isinstance(event, AiocqhttpMessageEvent)
            client = event.bot
            url_result = await client.api.call_action(
                "get_group_file_url", group_id=group_id, file_id=file_id
            )
            payload = _normalize_action_payload(url_result)
            masked_result = payload if payload else url_result
            if isinstance(masked_result, dict) and isinstance(
                masked_result.get("url"), str
            ):
                masked_result = dict(masked_result)
                masked_result["url"] = _mask_url_for_log(masked_result["url"])

            if isinstance(payload, dict) and payload.get("url"):
                return True

            logger.warning(
                f"[{group_id}] 文件有效性检查返回非失效特征响应，按有效处理: "
                f"file_id={file_id}, response_type={type(url_result).__name__}, response={masked_result}"
            )
            return True
        except Exception as e:
            if _is_known_invalid_file_error(e):
                return False

            logger.warning(
                f"[{group_id}] 文件有效性检查异常但未命中失效特征，按有效处理: file_id={file_id}, "
                f"error_type={type(e).__name__}, error={e}"
            )
            return True

    async def _task_delayed_recheck(
        self,
        event: AstrMessageEvent,
        file_name: str,
        file_id: str,
        file_component: Comp.File,
        preview_text: str,
        custom_msg_id: str | None = None,
        upload_time: Optional[int] = None,
        check_config: dict = None,
        repack_config: dict = None,
        backup_config: dict = None,
        local_path: Optional[str] = None,
    ):
        """延时复核任务"""
        if check_config is None:
            check_config = {}
        if repack_config is None:
            repack_config = {}

        check_delay_seconds = check_config.get("check_delay_seconds", 300)
        await asyncio.sleep(check_delay_seconds)
        group_id = int(event.get_group_id())
        target_msg_id = custom_msg_id or event.message_obj.message_id

        # 复核前检查目标消息是否已被撤回
        if not await is_msg_still_available(event, target_msg_id):
            logger.debug(
                f"[{group_id}] 目标消息 {target_msg_id} 已撤回，停止延时复核流程。"
            )
            return

        logger.debug(f"[{group_id}] [阶段二] 开始延时复核: '{file_name}'")

        is_still_valid = await self._check_validity_via_gfs(event, file_id)

        # 仅在首次检查失败时做二次确认，避免瞬时波动把有效文件误判为失效
        if not is_still_valid:
            await asyncio.sleep(1)
            is_still_valid = await self._check_validity_via_gfs(event, file_id)

        if not is_still_valid:
            # 报告失效前再次检查原消息（或补档通知）是否还在
            if not await is_msg_still_available(event, target_msg_id):
                return

            # 补档文件不触发再次补档（防止循环）
            is_repack_file = bool(event.get_extra("_is_repack_file"))
            if is_repack_file:
                logger.warning(
                    f"⚠️ [{group_id}] [阶段二] 检测到补档文件 '{file_name}' 复核可能失效，检测结果不稳定，仅记录日志。"
                )
                return

            enable_emoji = check_config.get("enable_emoji", True)
            await react_to_msg(event, "357", enable_emoji)
            logger.error(
                f"❌ [{group_id}] [阶段二] 文件 '{file_name}' 在延时复核时确认已失效!"
            )
            try:
                failure_message = (
                    f"❌ 经 {format_seconds(check_delay_seconds)}后复核，"
                    f"文件「{file_name}」已失效。"
                )
                await event.send(
                    MessageChain(
                        [Comp.Reply(id=target_msg_id), Comp.Plain(failure_message)]
                    )
                )

                # 文件失效时，只要 local_path 可用就尝试补档
                # file_component 不为 None（用户文件）或有 local_path（Bot 文件预复制）
                if file_component or local_path:
                    repack_extensions_str = repack_config.get(
                        "repack_file_extensions", ""
                    ).strip()
                    repack_file_extensions = [
                        ext.strip().lower()
                        for ext in repack_extensions_str.split(",")
                        if ext.strip()
                    ]

                    file_ext = os.path.splitext(file_name)[1].lower().lstrip(".")

                    if not repack_extensions_str or file_ext in repack_file_extensions:
                        logger.info(
                            f"文件在延时复核时失效，触发重新打包任务 (文件类型: {file_ext})..."
                        )
                        async for msg_chain in self._repack_and_send_file(
                            event,
                            file_name,
                            file_component,
                            repack_config,
                            local_path=local_path,
                        ):
                            await event.send(msg_chain)
                        # 补档后删除已失效的原文件
                        logger.info(
                            f"[{group_id}] 补档完成，已创建 10 分钟后的延迟删除任务"
                        )
                        asyncio.create_task(
                            self._delayed_delete_file(
                                event, file_name, 600, upload_time, file_id=file_id
                            )
                        )

                elif not file_component and not local_path:
                    logger.debug(
                        f"[{group_id}] 该文件无 file_component 且无 local_path，无法再次补档"
                    )

                # 备份（仅备份失效文件模式，复核阶段也需要备份）
                if (
                    backup_config
                    and backup_config.get("target_sid")
                    and backup_config.get("only_invalid", False)
                ):
                    await backup_file_to_session(
                        self.plugin.context, file_name, backup_config, local_path
                    )

            except Exception as send_e:
                logger.error(
                    f"[{group_id}] [阶段二] 回复失效通知时再次发生错误: {send_e}"
                )

        else:
            logger.debug(
                f"✅ [{group_id}] [阶段二] 文件 '{file_name}' 延时复核通过，保持沉默。"
            )

    async def _delayed_delete_file(
        self,
        event: AstrMessageEvent,
        file_name: str,
        delay: int,
        target_time: Optional[int] = None,
        file_id: Optional[str] = None,
    ):
        """延迟删除群文件。有 file_id 则直接删除，否则通过文件名查询。"""
        await asyncio.sleep(delay)
        group_id = int(event.get_group_id())

        if file_id:
            await self._delete_group_file(event, file_id, file_name)
        else:
            current_file_id = await self._search_file_id_by_name(
                event, file_name, target_time
            )
            if current_file_id:
                await self._delete_group_file(event, current_file_id, file_name)
            else:
                logger.warning(
                    f"[{group_id}] 延迟删除任务取消: 无法查询到文件 '{file_name}' ID，可能已被手动删除"
                )

    async def _save_cache_to_kv(self):
        # 只保存当前活跃群的缓存（在白名单中或未设白名单）
        whitelist = self.plugin.group_whitelist
        active_gids = set(self._cache.keys())
        if whitelist:
            active_gids &= {str(gid) for gid in whitelist}

        cached_groups = []
        for gid in active_gids:
            cache = self._cache.get(gid)
            if not cache or not cache.get("has_modify_time", False):
                continue
            try:
                await self.plugin.put_kv_data(
                    f"astrbot_plugin_file_checker_cache_{gid}", cache
                )
                cached_groups.append(gid)
            except Exception as e:
                logger.error(f"[KV保存] 缓存写入失败 group={gid}: {e}")

        # 清理非活跃群的残留 KV
        try:
            old_list = await self.plugin.get_kv_data(
                "astrbot_plugin_file_checker_groups", []
            )
            if old_list and isinstance(old_list, list):
                stale = set(old_list) - set(cached_groups)
                for sgid in stale:
                    try:
                        await self.plugin.delete_kv_data(
                            f"astrbot_plugin_file_checker_cache_{sgid}"
                        )
                        logger.debug(f"[KV清理] 已删除残留缓存 group={sgid}")
                    except Exception as e:
                        logger.warning(f"[KV清理] 删除残留缓存失败 group={sgid}: {e}")
        except Exception as e:
            logger.warning(f"[KV清理] 查询旧群组列表失败: {e}")

        if cached_groups:
            try:
                await self.plugin.put_kv_data(
                    "astrbot_plugin_file_checker_groups", cached_groups
                )
            except Exception as e:
                logger.error(f"[KV保存] 群组列表写入失败: {e}")

    def _patch_relative_path(self, cache: dict):
        if not isinstance(cache, dict):
            return
        flat_index = cache.get("flat_index")
        if not isinstance(flat_index, dict):
            return
        for fid, info in flat_index.items():
            if not isinstance(info, dict):
                continue
            if info.get("relative_path"):
                continue
            fname = info.get("file_name") or ""
            pname = info.get("parent_folder_name") or "根目录"
            if pname and pname != "根目录":
                info["relative_path"] = f"{pname}/{fname}"
            else:
                info["relative_path"] = fname

    async def _load_cache_from_kv(self):
        try:
            group_ids = await self.plugin.get_kv_data(
                "astrbot_plugin_file_checker_groups", []
            )
            if not group_ids or not isinstance(group_ids, list):
                return
            for gid in group_ids:
                try:
                    data = await self.plugin.get_kv_data(
                        f"astrbot_plugin_file_checker_cache_{gid}", None
                    )
                    if data and isinstance(data, dict) and data.get("has_modify_time"):
                        self._patch_relative_path(data)
                        self._cache[str(gid)] = data
                        logger.debug(
                            f"[KV加载] group={gid} 从 KV 恢复缓存 ({len(data.get('flat_index', {}))} 个文件)"
                        )
                except Exception as e:
                    logger.error(f"[KV加载] 单群组加载失败 group={gid}: {e}")
        except Exception as e:
            logger.error(f"[KV加载] 加载群组列表失败: {e}")

    async def _load_cache_from_kv_for_group(self, group_id: str):
        try:
            data = await self.plugin.get_kv_data(
                f"astrbot_plugin_file_checker_cache_{group_id}", None
            )
            if data and isinstance(data, dict) and data.get("has_modify_time"):
                self._patch_relative_path(data)
                self._cache[group_id] = data
                logger.debug(
                    f"[KV加载] group={group_id} 从 KV 恢复缓存 ({len(data.get('flat_index', {}))} 个文件)"
                )
                return True
        except Exception as e:
            logger.error(f"[KV加载] group={group_id}: {e}")
        return False

    async def handle_invalid_file(
        self,
        event: AstrMessageEvent,
        file_name: str,
        file_component: Comp.File,
        preview_text: str,
        extra_info: str,
        pdf_preview_images: List[str],
        upload_time: Optional[int],
        check_config: dict,
        repack_config: dict,
        local_path: Optional[str] = None,
        file_id: Optional[str] = None,
    ):
        """处理失效文件的逻辑：贴表情、发送通知、触发补档"""

        enable_emoji = check_config.get("enable_emoji", True)

        if not await is_msg_still_available(event, event.message_obj.message_id):
            return
        await react_to_msg(event, "357", enable_emoji)

        # 构建失效通知文案
        preview_config = get_group_config(
            self.plugin.config, str(event.get_group_id()), "preview_module"
        )
        failure_message = build_notification_text(
            file_name, False, preview_text, extra_info, preview_config
        )

        # 使用合并转发发送 PDF 预览（如果有）
        if pdf_preview_images:
            for msg in self.plugin.preview.send_pdf_preview(
                event, failure_message, pdf_preview_images
            ):
                yield msg
        else:
            yield event.chain_result(
                [
                    Comp.Reply(id=event.message_obj.message_id),
                    Comp.Plain(failure_message),
                ]
            )

        # 文件失效时，只要 file_component 或 local_path 可用就尝试补档
        if file_component or local_path:
            repack_extensions_str = repack_config.get(
                "repack_file_extensions", ""
            ).strip()
            repack_file_extensions = [
                ext.strip().lower()
                for ext in repack_extensions_str.split(",")
                if ext.strip()
            ]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip(".")

            if not repack_extensions_str or file_ext in repack_file_extensions:
                logger.info(
                    f"文件失效，触发重新打包任务 (文件类型: {file_ext if file_ext else '无后缀'})..."
                )
                async for msg in self._repack_and_send_file(
                    event,
                    file_name,
                    file_component,
                    repack_config,
                    local_path=local_path,
                ):
                    yield msg
                asyncio.create_task(
                    self._delayed_delete_file(
                        event, file_name, 600, upload_time, file_id=file_id
                    )
                )

import asyncio
import os
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional
import time
import chardet
import subprocess
import re
import pypdfium2 as pdfium

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent


class GroupFileCheckerPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}

        # 全局配置
        self.group_whitelist: List[int] = self.config.get("group_whitelist", [])
        self.group_whitelist = [int(gid) for gid in self.group_whitelist]
        self.file_size_threshold_mb: int = self.config.get("file_size_threshold_mb", 100)
        self.enable_emoji: bool = self.config.get("enable_emoji", True)

        # 有效性检查设置
        validity_check = self.config.get("validity_check", {})
        self.notify_on_success: bool = validity_check.get("notify_on_success", True)
        self.pre_check_delay_seconds: int = validity_check.get("pre_check_delay_seconds", 5)
        self.check_delay_seconds: int = validity_check.get("check_delay_seconds", 300)
        self.enable_duplicate_check: bool = validity_check.get("enable_duplicate_check", False)

        # 预览与转换设置
        preview_conversion = self.config.get("preview_conversion", {})
        self.preview_length: int = preview_conversion.get("preview_length", 500)
        self.pdf_preview_pages: int = preview_conversion.get("pdf_preview_pages", 0)
        self.enable_zip_preview: bool = preview_conversion.get("enable_zip_preview", True)
        self.zip_extraction_size_limit_mb: int = preview_conversion.get("zip_extraction_size_limit_mb", 100)
        self.default_zip_password: str = preview_conversion.get("default_zip_password", "")
        self.enable_auto_convert_image: bool = preview_conversion.get("enable_auto_convert_image", False)
        self.auto_convert_video_threshold_mb: int = preview_conversion.get("auto_convert_video_threshold_mb", 0)

        # 失效文件补档设置
        file_repack = self.config.get("file_repack", {})
        repack_extensions_str: str = file_repack.get("repack_file_extensions", "")
        self.repack_file_extensions: List[str] = [ext.strip().lower() for ext in repack_extensions_str.split(",") if ext.strip()]
        self.repack_zip_password: str = file_repack.get("repack_zip_password", "")

        # 7za 支持的压缩格式（不包括 RAR5）
        self.supported_archive_formats = (
            '.zip', '.7z', '.tar', '.gz', '.bz2', '.xz',
            '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
            '.iso', '.wim', '.rar'
        )

        # 支持文本预览的文件格式
        self.supported_text_formats = (
            # 文档类
            '.txt', '.md', '.log', '.epub',
            # 配置类
            '.json', '.xml', '.yaml', '.yml', '.ini', '.conf', '.cfg', '.toml',
            # 代码类
            '.py', '.js', '.java', '.c', '.cpp', '.h', '.go', '.rs', '.php', '.rb', '.sh', '.bash',
            '.html', '.htm', '.css', '.jsx', '.tsx', '.ts', '.vue', '.sql',
            # 数据类
            '.csv', '.properties', '.env'
        )

        # 图片转换大小限制，超过15MB会不稳定
        self.image_convert_max_size_mb = 15

        self.temp_dir = os.path.join(StarTools.get_data_dir("astrbot_plugin_file_checker"), "temp")
        os.makedirs(self.temp_dir, exist_ok=True)

        # 文件检查间隔控制：多个文件之间间隔0.3秒
        self.check_interval = 0.3  # 固定间隔300ms
        self.last_check_time = None  # 上次检查时间，None表示首次检查

        self.download_semaphore = asyncio.Semaphore(5)
        logger.info("QQ 文件预览插件已加载。")

    def _find_file_component(self, event: AstrMessageEvent) -> Optional[Comp.File]:
        for segment in event.get_messages():
            if isinstance(segment, Comp.File):
                return segment
        return None

    def _fix_zip_filename(self, filename: str) -> str:
        try:
            return filename.encode('cp437').decode('gbk')
        except (UnicodeEncodeError, UnicodeDecodeError):
            return filename
    
    def _is_video_file(self, filename: str) -> bool:
        """检测文件是否为视频格式（仅支持 mp4）"""
        file_ext = os.path.splitext(filename)[1].lower()
        return file_ext == '.mp4'
    
    def _is_image_file(self, filename: str) -> bool:
        """检测文件是否为图片格式（支持 jpg/jpeg/png/gif/webp）"""
        file_ext = os.path.splitext(filename)[1].lower()
        return file_ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']
    
    def _is_pdf_file(self, filename: str) -> bool:
        """检测文件是否为 PDF 格式"""
        file_ext = os.path.splitext(filename)[1].lower()
        return file_ext == '.pdf'
    
    async def _delete_group_file(self, event: AstrMessageEvent, file_id: str, file_name: str) -> bool:
        """删除群文件"""
        group_id = int(event.get_group_id())
        try:
            client = event.bot
            delete_result = await client.api.call_action('delete_group_file', group_id=group_id, file_id=file_id)
            
            if delete_result and delete_result.get('transGroupFileResult', {}).get('result', {}).get('retCode') == 0:
                logger.info(f"[{group_id}] ✅ 成功删除群文件: {file_name}")
                return True
            else:
                logger.warning(f"[{group_id}] ⚠️ 删除群文件失败: {file_name}")
                return False
        except Exception as e:
            logger.error(f"[{group_id}] ❌ 删除群文件时发生错误: {e}", exc_info=True)
            return False
    
    async def _convert_file_to_media(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, file_size: int, media_type: str):
        """
        将文件转换为媒体形式发送（支持视频和图片）
        
        Args:
            media_type: 'video' 或 'image'
        """
        group_id = int(event.get_group_id())
        local_file_path = None
        
        try:
            media_name = "视频" if media_type == "video" else "图片"
            emoji = "🎬" if media_type == "video" else "🖼️"
            logger.info(f"[{group_id}] {emoji} 开始{media_name}转换流程: {file_name}")
            
            async with self.download_semaphore:
                local_file_path = await file_component.get_file()
            
            if not local_file_path or not os.path.exists(local_file_path):
                logger.error(f"[{group_id}] ❌ 下载{media_name}文件失败")
                return
            
            file_size_mb = file_size / (1024 * 1024)
            absolute_path = os.path.abspath(local_file_path)
            
            logger.debug(f"[{group_id}] 📤 准备以{media_name}形式发送文件 ({file_size_mb:.2f} MB): {absolute_path}")
            
            if media_type == "video":
                media_message = [Comp.Video(file=f"file:///{absolute_path}")]
            else:  # image
                media_message = [Comp.Image.fromFileSystem(absolute_path)]
            
            yield event.chain_result(media_message)
            
            logger.info(f"[{group_id}] ✅ {media_name}发送成功，将在 30 分钟后删除群文件和本地缓存")
            
            # 30分钟后删除群文件和本地缓存
            delete_delay = 1800  # 30分钟
            asyncio.create_task(self._delayed_cleanup(event, file_name, local_file_path, delete_delay))
            
            return  # 转换成功
            
        except Exception as e:
            media_name = "视频" if media_type == "video" else "图片"
            logger.error(f"[{group_id}] ❌ {media_name}发送失败: {e}", exc_info=True)
            if local_file_path and os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                    logger.debug(f"[{group_id}] 🗑️ 已清理下载失败的本地{media_name}缓存")
                except OSError:
                    pass
            return  # 发送失败
    
    async def _delayed_cleanup(self, event: AstrMessageEvent, file_name: str, local_path: str, delay: int):
        """延迟清理群文件和本地文件"""
        await asyncio.sleep(delay)
        
        group_id = int(event.get_group_id())
        logger.debug(f"[{group_id}] 开始延迟清理视频文件: {file_name}")
        
        # 通过文件名查询最新的 file_id
        file_id = await self._search_file_id_by_name(event, file_name)
        
        if file_id:
            await self._delete_group_file(event, file_id, file_name)
        else:
            logger.error(f"[{group_id}] ❌ 无法查询到文件ID，可能文件已被删除或移动")
        
        # 删除本地文件
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.debug(f"[{group_id}] 🗑️ 已删除本地视频缓存: {os.path.basename(local_path)}")
            except OSError as e:
                logger.warning(f"[{group_id}] ⚠️ 删除本地视频缓存失败: {e}")
    
    async def _search_file_id_by_name(self, event: AstrMessageEvent, file_name: str, target_time: Optional[int] = None) -> Optional[str]:
        group_id = int(event.get_group_id())
        
        try:
            client = event.bot
            folders_to_scan = [{'folder_id': '/', 'folder_name': '根目录'}]
            matched_files = []
            
            while folders_to_scan:
                current_folder = folders_to_scan.pop(0)
                current_folder_id = current_folder['folder_id']
                
                if current_folder_id == '/':
                    result = await client.api.call_action('get_group_root_files', group_id=group_id)
                else:
                    result = await client.api.call_action('get_group_files_by_folder', group_id=group_id, folder_id=current_folder_id, file_count=1000)
                
                if not isinstance(result, dict):
                    continue
                
                for file_info in result.get('files', []):
                    if file_info.get('file_name') == file_name:
                        matched_files.append(file_info)
                
                for folder_info in result.get('folders', []):
                    folders_to_scan.append(folder_info)
            
            if not matched_files:
                logger.warning(f"[{group_id}] 未找到文件 '{file_name}'")
                return None
                
            if len(matched_files) == 1 or target_time is None:
                file_id = matched_files[0].get('file_id')
                logger.debug(f"[{group_id}] 查询到文件 '{file_name}' 的 file_id: {file_id}")
                return file_id
                
            # 如果有多个同名文件且提供了 target_time，寻找 modify_time 最接近的
            closest_file = None
            min_diff = float('inf')
            for f in matched_files:
                modify_time = f.get('modify_time', 0)
                diff = abs(modify_time - target_time)
                if diff < min_diff:
                    min_diff = diff
                    closest_file = f
                    
            if closest_file:
                file_id = closest_file.get('file_id')
                logger.debug(f"[{group_id}] 存在多个同名文件，根据时间戳匹配到最接近的文件 '{file_name}' 的 file_id: {file_id} (时间差: {min_diff}秒)")
                return file_id
                
            return None
        except Exception as e:
            logger.error(f"[{group_id}] 通过文件名搜索文件ID时出错: {e}", exc_info=True)
            return None

    async def _is_msg_still_available(self, event: AstrMessageEvent, msg_id: str) -> bool:
        """校验消息是否仍可获取"""
        if not msg_id or not isinstance(event, AiocqhttpMessageEvent):
            return True

        client = getattr(event, "bot", None)
        api = getattr(client, "api", None) if client else None
        if api is None:
            return True

        try:
            message_id = int(msg_id)
        except (TypeError, ValueError):
            return True

        try:
            detail = await api.call_action("get_msg", message_id=message_id)
        except Exception as e:
            err_text = str(e).lower()
            if any(k in err_text for k in ("not found", "not exist", "不存在", "撤回", "invalid")):
                logger.debug(f"[FileChecker] get_msg 指示消息不可用/已撤回: msg_id={message_id}")
                return False
            logger.debug(f"[FileChecker] get_msg 调用异常但按可用处理: msg_id={message_id}, err={e}")
            return True

        if isinstance(detail, dict) and isinstance(detail.get("data"), dict):
            detail = detail["data"]

        if not isinstance(detail, dict):
            logger.debug(f"[FileChecker] get_msg 返回结构异常，判定不可用: msg_id={message_id}")
            return False

        msg_content = detail.get("message")
        if msg_content is None:
            logger.debug(f"[FileChecker] get_msg 未返回 message 字段，判定不可用: msg_id={message_id}")
            return False
        if isinstance(msg_content, (list, str)) and len(msg_content) == 0:
            logger.debug(f"[FileChecker] get_msg 返回空内容，判定不可用: msg_id={message_id}")
            return False

        return True

    async def _check_if_file_exists_by_size(self, event: AstrMessageEvent, file_name: str, file_size: int, upload_time: int) -> List[Dict]:
        group_id = int(event.get_group_id())
        
        client = event.bot
        all_files_dict = {}
        folders_to_scan = [{'folder_id': '/', 'folder_name': '根目录'}]
        
        while folders_to_scan:
            current_folder = folders_to_scan.pop(0)
            current_folder_id = current_folder['folder_id']
            current_folder_name = current_folder['folder_name']
            
            try:
                if current_folder_id == '/':
                    result = await client.api.call_action('get_group_root_files', group_id=group_id)
                else:
                    result = await client.api.call_action('get_group_files_by_folder', group_id=group_id, folder_id=current_folder_id, file_count=1000)

                if not isinstance(result, dict):
                    logger.warning(f"[{group_id}] API返回了意料之外的格式。")
                    continue
                
                for file_info in result.get('files', []):
                    file_info['parent_folder_name'] = current_folder_name
                    all_files_dict[file_info.get('file_id')] = file_info
                
                for folder_info in result.get('folders', []):
                    folders_to_scan.append(folder_info)

            except Exception as e:
                logger.error(f"[{group_id}] 遍历文件夹 '{current_folder['folder_name']}' 时出错: {e}", exc_info=True)
        
        logger.debug(f"[{group_id}] 遍历完成，共找到 {len(all_files_dict)} 个文件。")
        
        possible_duplicates = []
        for file_info in all_files_dict.values():
            if file_info.get('file_size') == file_size:
                possible_duplicates.append(file_info)

        logger.debug(f"[{group_id}] 共找到 {len(possible_duplicates)} 个大小匹配的候选项。")
        
        existing_files = []
        removed_files = []
        
        for f in possible_duplicates:
            file_modify_time = f.get('modify_time')
            
            if file_modify_time is not None:
                file_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_modify_time))
                
                if abs(file_modify_time - upload_time) <= 2:
                    removed_files.append(f)
                else:
                    existing_files.append(f)
            else:
                existing_files.append(f)

        if removed_files:
            logger.debug(f"[{group_id}] 已从候选项中排除自身文件，共 {len(removed_files)} 个。")
        
        if existing_files:
            logger.debug(f"[{group_id}] 最终确认 {len(existing_files)} 个真正的重复文件。")
            for f in existing_files:
                modify_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))
                logger.debug(
                    f"  ↳ 文件名: '{f.get('file_name', '未知')}'\n"
                    f"    文件ID: {f.get('file_id', '未知')}\n"
                    f"    大小: {f.get('file_size', '未知')}字节\n"
                    f"    上传者: {f.get('uploader_name', '未知')}\n"
                    f"    修改时间: {modify_time_str}\n"
                    f"    所属文件夹: {f.get('parent_folder_name', '根目录')}"
                )
        else:
            logger.debug(f"[{group_id}] 未找到真正的重复文件。")
        
        return existing_files
    
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=2)
    async def on_group_message(self, event: AstrMessageEvent, *args, **kwargs):
        """处理群消息事件"""
        group_id = int(event.get_group_id())
        if self.group_whitelist and group_id not in self.group_whitelist:
            return

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
                            logger.error(f"无法将文件大小 '{file_size}' 转换为整数，已跳过重复性检查。")
                            file_size = None

                    if file_name and file_id:
                        if file_size is not None and self.file_size_threshold_mb > 0:
                            file_size_mb = file_size / (1024 * 1024)
                            if file_size_mb > self.file_size_threshold_mb:
                                logger.debug(f"[{group_id}] 文件 '{file_name}' 大小 ({file_size_mb:.2f} MB) 超过处理阈值 ({self.file_size_threshold_mb} MB)，跳过所有处理。")
                                return
                        logger.debug(f"成功解析: 文件名='{file_name}', ID='{file_id}'")
                        file_component = self._find_file_component(event)
                        if not file_component:
                            logger.error("致命错误：无法在组件中找到对应的File对象！")
                            return

                        upload_time = raw_event_data.get("time", int(time.time()))

                        if self.enable_duplicate_check and file_size is not None:
                            logger.debug(f"[{group_id}] 新上传文件时间戳: {upload_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(upload_time))})")

                            existing_files = await self._check_if_file_exists_by_size(event, file_name, file_size, upload_time)
                            if existing_files:
                                if len(existing_files) == 1:
                                    existing_file = existing_files[0]
                                    reply_text = (
                                        f"💡 提醒：您发送的文件「{file_name}」可能与群文件中的「{existing_file.get('file_name', '未知文件名')}」重复。\n"
                                        f"  ↳ 上传者: {existing_file.get('uploader_name', '未知')}\n"
                                        f"  ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(existing_file.get('modify_time', 0)))}\n"
                                        f"  ↳ 所属文件夹: {existing_file.get('parent_folder_name', '根目录')}"
                                    )
                                    yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)])
                                else:
                                    reply_text = f"💡 提醒：您发送的文件「{file_name}」可能与群文件中以下 {len(existing_files)} 个文件重复：\n"
                                    for idx, file_info in enumerate(existing_files, 1):
                                        reply_text += (
                                            f"\n{idx}. {file_info.get('file_name', '未知文件名')}\n"
                                            f"    ↳ 上传者: {file_info.get('uploader_name', '未知')}\n"
                                            f"    ↳ 修改时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_info.get('modify_time', 0)))}\n"
                                            f"    ↳ 所属文件夹: {file_info.get('parent_folder_name', '根目录')}"
                                        )
                                    yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)])
                                break

                        if self.auto_convert_video_threshold_mb > 0 and file_size is not None:
                            if self._is_video_file(file_name):
                                file_size_mb = file_size / (1024 * 1024)
                                if file_size_mb > self.auto_convert_video_threshold_mb:
                                    logger.debug(f"[{group_id}] 视频文件 '{file_name}' ({file_size_mb:.2f} MB) 超过转换阈值 ({self.auto_convert_video_threshold_mb} MB)，跳过自动转换")

                        async for result in self._handle_file_check_flow(event, file_name, file_id, file_component, file_size, upload_time):
                            yield result
                        break
        except Exception as e:
            logger.error(f"【原始方式】处理消息时发生致命错误: {e}", exc_info=True)

    async def _repack_and_send_file(self, event: AstrMessageEvent, original_filename: str, file_component: Comp.File):
        base_name = os.path.basename(original_filename)
        if re.search(r'[\\/|*<>;"\x00-\x1F\x7F]', base_name):
            logger.error(f"文件名 '{original_filename}' 包含非安全字符，已跳过重新打包。")
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain("❌ 文件名包含不安全字符，已跳过重新打包。")])
            return
        
        repacked_file_path = None
        original_txt_path = None
        renamed_txt_path = None
        try:
            logger.info(f"开始为失效文件 {original_filename} 进行重新打包...")
            
            original_txt_path = await file_component.get_file()
            
            renamed_txt_path = os.path.join(self.temp_dir, original_filename)
            if os.path.exists(renamed_txt_path):
                os.remove(renamed_txt_path)
            os.rename(original_txt_path, renamed_txt_path)

            base_name = os.path.splitext(original_filename)[0]
            new_zip_name = f"{base_name}.zip"
            repacked_file_path = os.path.join(self.temp_dir, f"{int(time.time())}_{new_zip_name}")

            command = ['zip', '-j', repacked_file_path, renamed_txt_path]
            if self.repack_zip_password:
                command.extend(['-P', self.repack_zip_password])

            logger.debug(f"正在执行打包命令: {' '.join(command)}")
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                error_message = stderr.decode('utf-8')
                logger.error(f"使用 zip 命令打包文件时出错: {error_message}")
                yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(f"❌ 重新打包失败，错误信息：\n{error_message}")])
                return
            
            logger.debug(f"文件已重新打包至 {repacked_file_path}，准备发送...")
            
            reply_text = "已为您重新打包为ZIP文件发送："
            file_component_to_send = Comp.File(file=repacked_file_path, name=new_zip_name)
            
            new_msg_id = None
            
            # 补档通知使用框架的消息发送方式
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)])
            
            # 发送文件并获取文件消息的 ID
            if isinstance(event, AiocqhttpMessageEvent):
                try:
                    # 使用 call_action 发送文件以获取 message_id
                    absolute_path = os.path.abspath(repacked_file_path)
                    file_msg = f"[CQ:file,file=file:///{absolute_path},name={new_zip_name}]"
                    ret = await event.bot.api.call_action('send_group_msg', group_id=int(event.get_group_id()), message=file_msg)
                    if isinstance(ret, dict):
                        new_msg_id = str(ret.get("message_id") or ret.get("data", {}).get("message_id") or "")
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
                asyncio.create_task(self._task_delayed_recheck(event, new_zip_name, new_file_id, None, None, custom_msg_id=new_msg_id, upload_time=int(time.time())))
            else:
                logger.error("未能获取新文件的ID，无法进行延时复核。")
            
        except FileNotFoundError:
            logger.error("重新打包失败：容器内未找到 zip 命令。请安装 zip。")
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain("❌ 重新打包失败。容器内未找到 zip 命令，请联系管理员安装。")])
        except Exception as e:
            logger.error(f"重新打包并发送文件时出错: {e}", exc_info=True)
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain("❌ 重新打包并发送文件失败。")])
        finally:
            if repacked_file_path and os.path.exists(repacked_file_path):
                async def cleanup_file(path: str):
                    await asyncio.sleep(10)
                    try:
                        os.remove(path)
                        logger.debug(f"已清理临时文件: {path}")
                    except OSError as e:
                        logger.warning(f"删除临时文件 {path} 失败: {e}")
                asyncio.create_task(cleanup_file(repacked_file_path))

            if renamed_txt_path and os.path.exists(renamed_txt_path):
                try:
                    os.remove(renamed_txt_path)
                    logger.debug(f"已清理重命名后的临时文件: {renamed_txt_path}")
                except OSError as e:
                    logger.warning(f"删除临时文件 {renamed_txt_path} 失败: {e}")

    async def _react(self, event: AstrMessageEvent, emoji_id: str):
        """贴表情回应（仅支持 aiocqhttp）"""
        if not self.enable_emoji or not isinstance(event, AiocqhttpMessageEvent):
            return
        try:
            await event.bot.api.call_action(
                "set_msg_emoji_like",
                message_id=int(event.message_obj.message_id),
                emoji_id=int(emoji_id)
            )
        except Exception as e:
            logger.warning(f"[FileChecker] 贴表情回应失败 (emoji_id={emoji_id}): {e}")

    async def _handle_file_check_flow(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, file_size: Optional[int] = None, upload_time: Optional[int] = None):
        group_id = int(event.get_group_id())

        sender_id = event.get_sender_id()
        self_id = event.get_self_id()
        if sender_id == self_id:
            logger.debug(f"[{group_id}] 机器人发送的文件，直接跳过处理。")
            return

        # 等待间隔时间，确保多个文件处理之间有0.3秒间隔
        if self.last_check_time is not None:
            current_time = time.time()
            time_since_last_check = current_time - self.last_check_time
            if time_since_last_check < self.check_interval:
                wait_time = self.check_interval - time_since_last_check
                logger.debug(f"[{group_id}] 等待 {wait_time:.2f}秒后再处理文件")
                await asyncio.sleep(wait_time)
        self.last_check_time = time.time()

        await asyncio.sleep(self.pre_check_delay_seconds)
        
        # 预检前检查消息是否已被撤回
        if not await self._is_msg_still_available(event, event.message_obj.message_id):
            logger.debug(f"[{group_id}] 原消息已撤回，停止即时检查流程。")
            return

        logger.info(f"[{group_id}] [阶段一] 开始即时检查: '{file_name}'")

        is_gfs_valid = await self._check_validity_via_gfs(event, file_id)

        preview_text, preview_extra_info = await self._get_preview_for_file(file_name, file_component, file_size)

        # PDF 预览生成 (无论有效性，和文本预览保持一致)
        pdf_preview_nodes = []
        if self.pdf_preview_pages > 0 and self._is_pdf_file(file_name):
            logger.debug(f"[{group_id}] 📄 尝试生成 PDF 预览 ({self.pdf_preview_pages} 页)")
            local_pdf_path = None
            try:
                async with self.download_semaphore:
                    local_pdf_path = await file_component.get_file()
                
                if local_pdf_path and os.path.exists(local_pdf_path):
                    image_paths = await self._get_pdf_preview(local_pdf_path)
                    if image_paths:
                        sender_id = event.get_self_id()
                        for img_path in image_paths:
                            pdf_preview_nodes.append(Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Image.fromFileSystem(img_path)]))
                        
                        # 延迟清理图片
                        async def cleanup_images(paths):
                            await asyncio.sleep(60)
                            for p in paths:
                                try:
                                    if os.path.exists(p): os.remove(p)
                                except: pass
                        asyncio.create_task(cleanup_images(image_paths))
            except Exception as e:
                logger.error(f"[{group_id}] PDF 预览处理出错: {e}", exc_info=True)
            finally:
                if local_pdf_path and os.path.exists(local_pdf_path):
                    try: os.remove(local_pdf_path)
                    except: pass

        # 检查是否符合媒体转换条件
        should_convert_video = (
            self.auto_convert_video_threshold_mb > 0 
            and file_size is not None 
            and self._is_video_file(file_name)
            and (file_size / (1024 * 1024)) <= self.auto_convert_video_threshold_mb
        )
        should_convert_image = (
            self.enable_auto_convert_image
            and file_size is not None 
            and self._is_image_file(file_name)
            and (file_size / (1024 * 1024)) <= self.image_convert_max_size_mb
        )

        if is_gfs_valid:
            # 开了预览: 包含文字、PDF 或 媒体转换
            has_any_preview = bool(preview_text or pdf_preview_nodes or should_convert_video or should_convert_image)
            await self._react(event, "314" if has_any_preview else "320")
        else:
            if not await self._is_msg_still_available(event, event.message_obj.message_id):
                return
            await self._react(event, "357")

        if is_gfs_valid:
            if should_convert_video:
                logger.info(f"[{group_id}] 🎬 文件有效，符合视频转换条件，尝试转换")
                # 尝试转换，不管成功与否都继续正常流程
                async for result in self._convert_file_to_media(event, file_name, file_id, file_component, file_size, "video"):
                    yield result
            elif should_convert_image:
                logger.info(f"[{group_id}] 🖼️ 文件有效，符合图片转换条件，尝试转换")
                # 尝试转换，不管成功与否都继续正常流程
                async for result in self._convert_file_to_media(event, file_name, file_id, file_component, file_size, "image"):
                    yield result
            
            text_preview_enabled = self.preview_length > 0
            has_any_preview = bool(preview_text or pdf_preview_nodes)
            
            if self.notify_on_success and not text_preview_enabled:
                success_message = f"✅ 您发送的文件「{file_name}」初步检查有效。"
                yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(success_message)])
            elif self.notify_on_success and text_preview_enabled:
                success_message = f"✅ 您发送的文件「{file_name}」初步检查有效。"
                if preview_text:
                    # 文件结构列表不截断，普通文本预览才截断
                    is_file_structure = preview_extra_info == "文件结构"
                    if is_file_structure:
                        preview_text_short = preview_text
                    else:
                        preview_text_short = preview_text[:self.preview_length]
                    
                    success_message += f"\n{preview_extra_info}：\n{preview_text_short}"
                    if not is_file_structure and len(preview_text) > self.preview_length:
                        success_message += "..."
                
                if pdf_preview_nodes:
                    # 将文字通知作为合并转发的第一条记录
                    success_message += f"\n📄 PDF 预览图如下："
                    sender_id = event.get_self_id()
                    pdf_preview_nodes.insert(0, Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Plain(success_message)]))
                    yield event.chain_result([Comp.Nodes(nodes=pdf_preview_nodes)])
                    logger.info(f"[{group_id}] ✅ PDF 预览已发送 ({len(pdf_preview_nodes)-1} 页，包含文字通知)")
                else:
                    yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(success_message)])
            elif (not self.notify_on_success) and text_preview_enabled and has_any_preview:
                success_message = f"✅ 您发送的文件「{file_name}」初步检查有效。"
                if preview_text:
                    is_file_structure = preview_extra_info == "文件结构"
                    if is_file_structure:
                        preview_text_short = preview_text
                    else:
                        preview_text_short = preview_text[:self.preview_length]
                    
                    success_message += f"\n{preview_extra_info}：\n{preview_text_short}"
                    if not is_file_structure and len(preview_text) > self.preview_length:
                        success_message += "..."
                
                if pdf_preview_nodes:
                    success_message += f"\n📄 PDF 预览图如下："
                    sender_id = event.get_self_id()
                    pdf_preview_nodes.insert(0, Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Plain(success_message)]))
                    yield event.chain_result([Comp.Nodes(nodes=pdf_preview_nodes)])
                    logger.info(f"[{group_id}] ✅ PDF 预览已发送 ({len(pdf_preview_nodes)-1} 页，包含文字通知)")
                else:
                    yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(success_message)])

            logger.info(f"[{group_id}] 初步检查通过，已加入延时复核队列。")
            asyncio.create_task(self._task_delayed_recheck(event, file_name, file_id, file_component, preview_text, upload_time=upload_time))
        else:
            logger.error(f"❌ [{group_id}] [阶段一] 文件 '{file_name}' 即时检查已失效!")
            try:
                failure_message = f"⚠️ 您发送的文件「{file_name}」已失效。"
                if preview_text:
                    # 文件结构列表不截断，普通文本预览才截断
                    is_file_structure = preview_extra_info == "文件结构"
                    if is_file_structure:
                        preview_text_short = preview_text
                    else:
                        preview_text_short = preview_text[:self.preview_length]
                    
                    failure_message += f"\n{preview_extra_info}：\n{preview_text_short}"
                    if not is_file_structure and len(preview_text) > self.preview_length:
                        failure_message += "..."
                
                if pdf_preview_nodes:
                    # 将文字通知作为合并转发的第一条记录
                    failure_message += f"\n📄 PDF 预览图如下："
                    sender_id = event.get_self_id()
                    pdf_preview_nodes.insert(0, Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Plain(failure_message)]))
                    yield event.chain_result([Comp.Nodes(nodes=pdf_preview_nodes)])
                    logger.info(f"[{group_id}] ✅ PDF 预览已发送 ({len(pdf_preview_nodes)-1} 页，包含文字通知)")
                else:
                    yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(failure_message)])

                if self.repack_file_extensions:
                    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
                    if file_ext in self.repack_file_extensions:
                        logger.info(f"文件即时检查失效，触发重新打包任务 (文件类型: {file_ext})...")
                        async for result in self._repack_and_send_file(event, file_name, file_component):
                            yield result
                        # 补档后删除已失效的原文件
                        logger.info(f"[{group_id}] 补档完成，已创建 10 分钟后的延迟删除任务")
                        asyncio.create_task(self._delayed_delete_file(event, file_name, 600, upload_time))
            except Exception as send_e:
                logger.error(f"[{group_id}] [阶段一] 回复失效通知时再次发生错误: {send_e}")

    async def _check_validity_via_gfs(self, event: AstrMessageEvent, file_id: str) -> bool:
        """检查文件有效性"""
        group_id = int(event.get_group_id())
        try:
            assert isinstance(event, AiocqhttpMessageEvent)
            client = event.bot
            url_result = await client.api.call_action('get_group_file_url', group_id=group_id, file_id=file_id)
            return bool(url_result and url_result.get('url'))
        except Exception:
            return False

    def _get_preview_from_bytes(self, content_bytes: bytes) -> tuple[str, str]:
        try:
            detection = chardet.detect(content_bytes)
            encoding = detection.get('encoding', 'utf-8') or 'utf-8'
            
            if encoding and detection['confidence'] > 0.7:
                decoded_text = content_bytes.decode(encoding, errors='ignore').strip()
                return decoded_text, encoding
            
            if encoding:
                decoded_text = content_bytes.decode(encoding, errors='ignore').strip()
                return decoded_text, f"{encoding} (低置信度回退)"
            
            return "", "未知"
            
        except Exception:
            return "", "未知"
            
    def _is_text_file(self, file_name: str) -> bool:
        """检查文件是否为支持的文本格式"""
        file_lower = file_name.lower()
        return any(file_lower.endswith(ext) for ext in self.supported_text_formats)
    
    def _is_archive_file(self, file_name: str) -> bool:
        """检查文件是否为支持的压缩格式"""
        file_lower = file_name.lower()
        return any(file_lower.endswith(ext) for ext in self.supported_archive_formats)
    
    async def _get_preview_from_archive(self, file_path: str, file_name: str) -> tuple[str, str]:
        """通用压缩包预览方法，支持多种格式"""
        extract_path = os.path.join(self.temp_dir, f"extract_{int(time.time())}")
        os.makedirs(extract_path, exist_ok=True)
        
        archive_type = "压缩包"
        if file_name.lower().endswith('.zip'):
            archive_type = "ZIP"
        elif file_name.lower().endswith('.7z'):
            archive_type = "7Z"
        elif file_name.lower().endswith('.rar'):
            archive_type = "RAR"
        elif any(file_name.lower().endswith(ext) for ext in ['.tar.gz', '.tgz']):
            archive_type = "TAR.GZ"
        elif any(file_name.lower().endswith(ext) for ext in ['.tar.bz2', '.tbz2']):
            archive_type = "TAR.BZ2"
        elif any(file_name.lower().endswith(ext) for ext in ['.tar.xz', '.txz']):
            archive_type = "TAR.XZ"
        elif file_name.lower().endswith('.tar'):
            archive_type = "TAR"
        
        try:
            logger.info(f"正在尝试解压 {archive_type} 文件（无密码）...")
            command_no_pwd = ["7za", "x", file_path, f"-o{extract_path}", "-y"]
            process = await asyncio.create_subprocess_exec(
                *command_no_pwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                if self.default_zip_password:
                    logger.info(f"无密码解压 {archive_type} 失败，正在尝试使用默认密码...")
                    command_with_pwd = ["7za", "x", file_path, f"-o{extract_path}", f"-p{self.default_zip_password}", "-y"]
                    process = await asyncio.create_subprocess_exec(
                        *command_with_pwd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    stdout, stderr = await process.communicate()
                    
                    if process.returncode != 0:
                        error_message = stderr.decode('utf-8')
                        logger.error(f"使用默认密码解压 {archive_type} 失败: {error_message}")
                        return "", f"{archive_type} 解压失败"
                else:
                    error_message = stderr.decode('utf-8')
                    logger.error(f"使用 7za 命令解压 {archive_type} 失败且未设置默认密码: {error_message}")
                    return "", f"{archive_type} 解压失败"

            all_extracted_files = []
            for root, dirs, files in os.walk(extract_path):
                for f in files:
                    full_path = os.path.join(root, f)
                    all_extracted_files.append(full_path)
            
            # 查找所有支持的文本文件
            text_files = [f for f in all_extracted_files 
                         if any(f.lower().endswith(ext) for ext in self.supported_text_formats)]
            
            if not text_files:
                # 如果没有找到文本文件，输出压缩包的文件结构
                if not all_extracted_files:
                    return "", f"{archive_type} 为空或解压失败"
                
                # 构建文件结构树
                file_structure = [f"📦 {archive_type} 文件结构："]
                for f_path in sorted(all_extracted_files):
                    relative_path = os.path.relpath(f_path, extract_path)
                    try:
                        file_size = os.path.getsize(f_path)
                        # 格式化文件大小
                        if file_size < 1024:
                            size_str = f"{file_size} B"
                        elif file_size < 1024 * 1024:
                            size_str = f"{file_size / 1024:.2f} KB"
                        elif file_size < 1024 * 1024 * 1024:
                            size_str = f"{file_size / (1024 * 1024):.2f} MB"
                        else:
                            size_str = f"{file_size / (1024 * 1024 * 1024):.2f} GB"
                        
                        # 计算缩进层级
                        depth = relative_path.count(os.sep)
                        indent = "  " * depth
                        file_name_only = os.path.basename(relative_path)
                        file_structure.append(f"{indent}├─ {file_name_only} ({size_str})")
                    except Exception as e:
                        logger.warning(f"获取文件 {relative_path} 信息失败: {e}")
                        continue
                
                structure_text = "\n".join(file_structure)
                return structure_text, "文件结构"
                
            # 优先级排序：README 文件 > txt/md 文件 > 其他文本文件 > 按文件大小
            def sort_priority(file_path):
                basename = os.path.basename(file_path).lower()
                # 第一优先级：README 文件
                if basename.startswith('readme'):
                    return (0, 0, os.path.getsize(file_path))
                # 第二优先级：txt 和 md 文件
                elif basename.endswith('.txt'):
                    return (1, 0, os.path.getsize(file_path))
                elif basename.endswith('.md'):
                    return (1, 1, os.path.getsize(file_path))
                # 第三优先级：其他文本文件，按大小排序（小文件优先）
                else:
                    return (2, 0, os.path.getsize(file_path))
            
            text_files.sort(key=sort_priority)
            first_text_file = text_files[0]
            safe_text_name = os.path.basename(first_text_file)
            
            if re.search(r'[\\/|*<>;"\x00-\x1F\x7F]', safe_text_name):
                logger.error(f"解压出的文件名 '{safe_text_name}' 包含非安全字符，跳过预览。")
                return "", "解压出的文件名不安全"

            extracted_text_path = first_text_file  # 已经是完整路径了
            
            with open(extracted_text_path, 'rb') as f:
                content_bytes = f.read(self.preview_length * 4)
            
            preview_text, encoding = self._get_preview_from_bytes(content_bytes)
            extra_info = f"已解压「{safe_text_name}」(格式 {encoding})"
            return preview_text, extra_info
            
        except FileNotFoundError:
            logger.error("解压失败：容器内未找到 7za 命令。请安装 p7zip-full。")
            return "", "未安装 7za"
        except Exception as e:
            logger.error(f"处理 {archive_type} 文件时发生未知错误: {e}", exc_info=True)
            return "", "未知错误"
        finally:
            if extract_path and os.path.exists(extract_path):
                try:
                    for root, dirs, files in os.walk(extract_path, topdown=False):
                        for name in files:
                            os.remove(os.path.join(root, name))
                        for name in dirs:
                            os.rmdir(os.path.join(root, name))
                    os.rmdir(extract_path)
                    logger.debug(f"已清理临时文件夹: {extract_path}")
                except Exception as e:
                    logger.warning(f"删除临时文件夹 {extract_path} 失败: {e}")

    def _is_epub_file(self, file_name: str) -> bool:
        """检查文件是否为 EPUB 格式"""
        return file_name.lower().endswith('.epub')

    def _extract_epub_text(self, epub_path: str, max_chars: int = 1000) -> str:
        """
        从 EPUB 文件中提取纯文本预览内容。
        使用标准库实现，无需额外依赖。
        """
        if not zipfile.is_zipfile(epub_path):
            return "错误：不是有效的 EPUB 文件（无效的 ZIP 结构）。"

        try:
            with zipfile.ZipFile(epub_path, 'r') as z:
                # 1. 找到 container.xml 以获取 .opf 文件路径
                try:
                    container_content = z.read('META-INF/container.xml')
                    root = ET.fromstring(container_content)
                    # 命名空间处理
                    ns = {'ns': 'urn:oasis:names:tc:opendocument:xmlns:container'}
                    rootfile = root.find('.//ns:rootfile', ns)
                    if rootfile is None:
                        return "错误：EPUB 结构异常，未找到 rootfile。"
                    opf_path = rootfile.attrib.get('full-path')
                except Exception as e:
                    return f"错误：解析 container.xml 失败: {e}"

                if not opf_path:
                    return "错误：未找到 OPF 文件路径。"

                # 2. 解析 .opf 文件
                try:
                    opf_content = z.read(opf_path)
                    opf_dir = os.path.dirname(opf_path)
                    root = ET.fromstring(opf_content)
                    
                    # OPF 命名空间通常是 http://www.idpf.org/2007/opf
                    ns = {'opf': 'http://www.idpf.org/2007/opf'}
                    
                    # 获取 manifest (id -> href 映射)
                    manifest = {}
                    for item in root.findall('.//opf:manifest/opf:item', ns):
                        item_id = item.attrib.get('id')
                        href = item.attrib.get('href')
                        if item_id and href:
                            manifest[item_id] = href
                    
                    # 获取 spine (阅读顺序)
                    spine_items = []
                    for itemref in root.findall('.//opf:spine/opf:itemref', ns):
                        idref = itemref.attrib.get('idref')
                        if idref in manifest:
                            # 处理路径拼接，注意 EPUB 内部使用正斜杠
                            full_href = os.path.join(opf_dir, manifest[idref]).replace('\\', '/')
                            spine_items.append(full_href)
                except Exception as e:
                    return f"错误：解析 OPF 文件失败: {e}"

                # 3. 按顺序读取 spine 中的内容并提取文本
                full_text = []
                current_len = 0
                
                # 定义 HTML 处理正则
                # 1. 匹配 <style> 和 <script> 块及其内容并删除
                re_scripts = re.compile(r'<(script|style).*?>.*?</\1>', re.DOTALL | re.IGNORECASE)
                # 2. 将块级标签转换为换行符
                re_block_tags = re.compile(r'<(p|div|br|li|h[1-6]|tr|blockquote|section|article).*?>', re.IGNORECASE)
                # 3. 匹配所有剩余 HTML 标签
                re_tags = re.compile(r'<[^>]+>')
                # 4. 匹配重复的空白字符（不包括换行）
                re_spaces = re.compile(r'[ \t\f\v]+')
                # 5. 匹配三个及以上的换行符
                re_newlines = re.compile(r'\n{3,}')

                for item_path in spine_items:
                    # 考虑到 EPUB 章节可能很大，我们每次提取完检查总长度
                    if current_len >= max_chars * 2: # 稍微多取一点点，方便后续精确截断
                        break
                    
                    try:
                        html_content = z.read(item_path).decode('utf-8', errors='ignore')
                        
                        # 0. 预处理：去掉脚本和样式，并将原有的换行符转为空格
                        text = re_scripts.sub('', html_content)
                        text = text.replace('\r', ' ').replace('\n', ' ')
                        
                        # 1. 将块级标签转为换行
                        text = re_block_tags.sub('\n', text)
                        
                        # 2. 去掉剩余标签
                        text = re_tags.sub('', text)
                        
                        # 3. 实体转换
                        text = text.replace('&nbsp;', ' ').replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&').replace('&quot;', '"')
                        
                        # 4. 压缩水平空白
                        text = re_spaces.sub(' ', text)
                        
                        # 5. 逐行处理：去掉每行首尾空格，并过滤掉空行
                        lines = []
                        for line in text.split('\n'):
                            stripped = line.strip()
                            if stripped:
                                lines.append(stripped)
                        
                        # 重新组合文本，并在段落间保留一个换行
                        text = '\n'.join(lines)
                        
                        if text:
                            full_text.append(text)
                            current_len += len(text)
                    except Exception:
                        continue
                
                # 合并所有章节内容
                result = "\n\n".join(full_text)
                # 再次确保没有过多的换行符
                result = re_newlines.sub('\n\n', result).strip()
                
                if not result:
                    return "（未提取到有效文本内容）"
                
                if len(result) > max_chars:
                    return result[:max_chars + 1]
                
                return result

        except Exception as e:
            logger.error(f"提取 EPUB 文本时出错: {e}", exc_info=True)
            return f"错误：提取失败: {e}"

    async def _get_preview_for_file(self, file_name: str, file_component: Comp.File, file_size: Optional[int] = None) -> tuple[str, str]:
        if self.preview_length <= 0:
            return "", ""
        is_text = self._is_text_file(file_name)
        is_epub = self._is_epub_file(file_name)
        is_archive = self.enable_zip_preview and self._is_archive_file(file_name)
        
        if not (is_text or is_epub or is_archive):
            return "", ""
        
        if is_archive and file_size is not None:
            archive_size_mb = file_size / (1024 * 1024)
            limit_mb = self.zip_extraction_size_limit_mb
            
            if limit_mb > 0 and archive_size_mb > limit_mb:
                logger.debug(f"压缩文件大小 ({archive_size_mb:.2f} MB) 超过配置的上限 ({limit_mb} MB)，跳过下载和解压预览。")
                return "", "文件过大，跳过解压"
        
        local_file_path = None
        try:
            async with self.download_semaphore:
                local_file_path = await file_component.get_file()
            
            if is_epub:
                preview_text = self._extract_epub_text(local_file_path, self.preview_length)
                return preview_text, "📖 EPUB 内容预览"

            if is_text:
                with open(local_file_path, 'rb') as f:
                    content_bytes = f.read(self.preview_length * 4)
                preview_text, encoding = self._get_preview_from_bytes(content_bytes)
                extra_info = f"📄 文本预览 ({encoding})"
                return preview_text, extra_info
            
            if is_archive:
                return await self._get_preview_from_archive(local_file_path, file_name)
        except Exception as e:
            logger.error(f"获取预览时下载或读取文件失败: {e}", exc_info=True)
            return "", ""
        finally:
            if local_file_path and os.path.exists(local_file_path):
                try:
                    os.remove(local_file_path)
                except OSError as e:
                    logger.warning(f"删除临时文件 {local_file_path} 失败: {e}")
        return "", ""

    async def _get_pdf_preview(self, file_path: str) -> List[str]:
        """使用 pypdfium2 生成 PDF 预览图"""
        image_paths = []
        try:
            pdf = pdfium.PdfDocument(file_path)
            num_pages = len(pdf)
            pages_to_render = min(num_pages, self.pdf_preview_pages)
            
            for i in range(pages_to_render):
                page = pdf[i]
                bitmap = page.render(scale=2)
                image_path = os.path.join(self.temp_dir, f"pdf_preview_{int(time.time())}_{i}.png")
                bitmap.to_pil().save(image_path)
                image_paths.append(image_path)
                page.close() # 释放资源
            pdf.close() # 释放资源
        except Exception as e:
            logger.error(f"生成 PDF 预览失败: {e}", exc_info=True)
        return image_paths

    async def _task_delayed_recheck(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, preview_text: str, custom_msg_id: str | None = None, upload_time: Optional[int] = None):
        """延时复核任务"""
        await asyncio.sleep(self.check_delay_seconds)
        group_id = int(event.get_group_id())
        target_msg_id = custom_msg_id or event.message_obj.message_id

        # 复核前检查目标消息是否已被撤回
        if not await self._is_msg_still_available(event, target_msg_id):
            logger.debug(f"[{group_id}] 目标消息 {target_msg_id} 已撤回，停止延时复核流程。")
            return

        logger.debug(f"[{group_id}] [阶段二] 开始延时复核: '{file_name}'")
        
        is_still_valid = await self._check_validity_via_gfs(event, file_id)
        
        if not is_still_valid:
            # 报告失效前再次检查原消息（或补档通知）是否还在
            if not await self._is_msg_still_available(event, target_msg_id):
                return
            
            await self._react(event, "357")
            logger.error(f"❌ [{group_id}] [阶段二] 文件 '{file_name}' 在延时复核时确认已失效!")
            try:
                failure_message = f"❌ 经 {self.check_delay_seconds} 秒后复核，您发送的文件「{file_name}」已失效。"
                await event.send(MessageChain([Comp.Reply(id=target_msg_id), Comp.Plain(failure_message)]))
                
                # 只有在 file_component 不为 None 时才尝试补档
                if file_component and self.repack_file_extensions:
                    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
                    if file_ext in self.repack_file_extensions:
                        logger.info(f"文件在延时复核时失效，触发重新打包任务 (文件类型: {file_ext})...")
                        async for msg_chain in self._repack_and_send_file(event, file_name, file_component):
                            await event.send(msg_chain)
                        # 补档后删除已失效的原文件
                        logger.info(f"[{group_id}] 补档完成，已创建 10 分钟后的延迟删除任务")
                        asyncio.create_task(self._delayed_delete_file(event, file_name, 600, upload_time))
                elif not file_component:
                    logger.debug(f"[{group_id}] 该文件为补档后的文件，无法再次补档")

            except Exception as send_e:
                logger.error(f"[{group_id}] [阶段二] 回复失效通知时再次发生错误: {send_e}")
        else:
            logger.debug(f"✅ [{group_id}] [阶段二] 文件 '{file_name}' 延时复核通过，保持沉默。")

    async def _delayed_delete_file(self, event: AstrMessageEvent, file_name: str, delay: int, target_time: Optional[int] = None):
        """延迟删除群文件"""
        await asyncio.sleep(delay)
        group_id = int(event.get_group_id())
        
        # 重新查询文件ID以确保准确删除
        current_file_id = await self._search_file_id_by_name(event, file_name, target_time)
        if current_file_id:
            await self._delete_group_file(event, current_file_id, file_name)
            logger.debug(f"[{group_id}] 延迟删除任务完成: {file_name}")
        else:
            logger.warning(f"[{group_id}] 延迟删除任务取消: 无法查询到文件 '{file_name}' ID，可能已被手动删除")

    async def terminate(self):
        logger.info("QQ 文件预览插件已卸载。")

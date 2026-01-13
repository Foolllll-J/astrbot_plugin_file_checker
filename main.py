import asyncio
import os
from typing import List, Dict, Optional
import time
import chardet
import subprocess
import re
import pypdfium2 as pdfium

from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent

@register(
    "astrbot_plugin_file_checker",
    "Foolllll",
    "群文件预览助手",
    "1.8.0",
    "https://github.com/Foolllll-J/astrbot_plugin_file_checker"
)
class GroupFileCheckerPlugin(Star):
    def __init__(self, context: Context, config: Optional[Dict] = None):
        super().__init__(context)
        self.config = config if config else {}
        self.group_whitelist: List[int] = self.config.get("group_whitelist", [])
        self.group_whitelist = [int(gid) for gid in self.group_whitelist]
        self.notify_on_success: bool = self.config.get("notify_on_success", True)
        self.pre_check_delay_seconds: int = self.config.get("pre_check_delay_seconds", 5)
        self.check_delay_seconds: int = self.config.get("check_delay_seconds", 300)
        self.preview_length: int = self.config.get("preview_length", 500)
        self.enable_duplicate_check: bool = self.config.get("enable_duplicate_check", False)
        self.enable_zip_preview: bool = self.config.get("enable_zip_preview", True)
        self.zip_extraction_size_limit_mb: int = self.config.get("zip_extraction_size_limit_mb", 100)
        self.default_zip_password: str = self.config.get("default_zip_password", "")
        
        # 7za 支持的压缩格式（不包括 RAR5）
        self.supported_archive_formats = (
            '.zip', '.7z', '.tar', '.gz', '.bz2', '.xz',
            '.tar.gz', '.tgz', '.tar.bz2', '.tbz2', '.tar.xz', '.txz',
            '.iso', '.wim', '.rar'
        )
        
        # 支持文本预览的文件格式
        self.supported_text_formats = (
            # 文档类
            '.txt', '.md', '.log',
            # 配置类
            '.json', '.xml', '.yaml', '.yml', '.ini', '.conf', '.cfg', '.toml',
            # 代码类
            '.py', '.js', '.java', '.c', '.cpp', '.h', '.go', '.rs', '.php', '.rb', '.sh', '.bash',
            '.html', '.htm', '.css', '.jsx', '.tsx', '.ts', '.vue', '.sql',
            # 数据类
            '.csv', '.properties', '.env'
        )
        repack_extensions_str: str = self.config.get("repack_file_extensions", "")
        self.repack_file_extensions: List[str] = [ext.strip().lower() for ext in repack_extensions_str.split(",") if ext.strip()]
        self.repack_zip_password: str = self.config.get("repack_zip_password", "")
        self.file_size_threshold_mb: int = self.config.get("file_size_threshold_mb", 100)
        
        # 媒体转换配置
        self.auto_convert_video_threshold_mb: int = self.config.get("auto_convert_video_threshold_mb", 0)
        self.enable_auto_convert_image = self.config.get("enable_auto_convert_image", False)
        self.pdf_preview_pages = self.config.get("pdf_preview_pages", 0)
        self.image_convert_max_size_mb = 15  # 图片转换大小限制，超过15MB会不稳定
        
        self.temp_dir = os.path.join(StarTools.get_data_dir("astrbot_plugin_file_checker"), "temp")
        os.makedirs(self.temp_dir, exist_ok=True)
        
        self.download_semaphore = asyncio.Semaphore(5)
        logger.info("插件 [群文件失效检查] 已加载。")

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
            
            logger.info(f"[{group_id}] 📤 准备以{media_name}形式发送文件 ({file_size_mb:.2f} MB): {absolute_path}")
            
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
                    logger.info(f"[{group_id}] 🗑️ 已清理下载失败的本地{media_name}缓存")
                except OSError:
                    pass
            return  # 发送失败
    
    async def _delayed_cleanup(self, event: AstrMessageEvent, file_name: str, local_path: str, delay: int):
        """延迟清理群文件和本地文件"""
        await asyncio.sleep(delay)
        
        group_id = int(event.get_group_id())
        logger.info(f"[{group_id}] 开始延迟清理视频文件: {file_name}")
        
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
                logger.info(f"[{group_id}] 🗑️ 已删除本地视频缓存: {os.path.basename(local_path)}")
            except OSError as e:
                logger.warning(f"[{group_id}] ⚠️ 删除本地视频缓存失败: {e}")
    
    async def _search_file_id_by_name(self, event: AstrMessageEvent, file_name: str) -> Optional[str]:
        group_id = int(event.get_group_id())
        
        try:
            client = event.bot
            file_list = await client.api.call_action('get_group_root_files', group_id=group_id)
            
            if not isinstance(file_list, dict) or 'files' not in file_list:
                logger.warning("get_group_root_files API调用返回了意料之外的格式。")
                return None
            
            for file_info in file_list.get('files', []):
                if file_info.get('file_name') == file_name:
                    file_id = file_info.get('file_id')
                    logger.debug(f"[{group_id}] 查询到文件 '{file_name}' 的 file_id: {file_id}")
                    return file_id
            
            logger.warning(f"[{group_id}] 未找到文件 '{file_name}'")
            return None
        except Exception as e:
            logger.error(f"[{group_id}] 通过文件名搜索文件ID时出错: {e}", exc_info=True)
            return None

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
            logger.info(f"[{group_id}] 最终确认 {len(existing_files)} 个真正的重复文件。")
            for f in existing_files:
                modify_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(f.get('modify_time', 0)))
                logger.info(
                    f"  ↳ 文件名: '{f.get('file_name', '未知')}'\n"
                    f"    文件ID: {f.get('file_id', '未知')}\n"
                    f"    大小: {f.get('file_size', '未知')}字节\n"
                    f"    上传者: {f.get('uploader_name', '未知')}\n"
                    f"    修改时间: {modify_time_str}\n"
                    f"    所属文件夹: {f.get('parent_folder_name', '根目录')}"
                )
        else:
            logger.info(f"[{group_id}] 未找到真正的重复文件。")
        
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
                        
                        if self.enable_duplicate_check and file_size is not None:
                            upload_time = raw_event_data.get("time", int(time.time()))
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

                        async for result in self._handle_file_check_flow(event, file_name, file_id, file_component, file_size):
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
            
            logger.info(f"文件已重新打包至 {repacked_file_path}，准备发送...")
            
            reply_text = "已为您重新打包为ZIP文件发送："
            file_component_to_send = Comp.File(file=repacked_file_path, name=new_zip_name)
            
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(reply_text)])
            
            yield event.chain_result([file_component_to_send])
            
            await asyncio.sleep(2)
            
            new_file_id = await self._search_file_id_by_name(event, new_zip_name)
            
            if new_file_id:
                logger.info(f"新文件发送成功，ID为 {new_file_id}，已加入延时复核队列。")
                asyncio.create_task(self._task_delayed_recheck(event, new_zip_name, new_file_id, None, None))
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
                        logger.info(f"已清理临时文件: {path}")
                    except OSError as e:
                        logger.warning(f"删除临时文件 {path} 失败: {e}")
                asyncio.create_task(cleanup_file(repacked_file_path))

            if renamed_txt_path and os.path.exists(renamed_txt_path):
                try:
                    os.remove(renamed_txt_path)
                    logger.info(f"已清理重命名后的临时文件: {renamed_txt_path}")
                except OSError as e:
                    logger.warning(f"删除临时文件 {renamed_txt_path} 失败: {e}")
    
    async def _handle_file_check_flow(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, file_size: Optional[int] = None):
        group_id = int(event.get_group_id())
        
        sender_id = event.get_sender_id()
        self_id = event.get_self_id()
        if sender_id == self_id:
            logger.info(f"[{group_id}] 机器人发送的文件，直接跳过处理。")
            return
        
        await asyncio.sleep(self.pre_check_delay_seconds)
        logger.info(f"[{group_id}] [阶段一] 开始即时检查: '{file_name}'")

        is_gfs_valid = await self._check_validity_via_gfs(event, file_id)

        preview_text, preview_extra_info = await self._get_preview_for_file(file_name, file_component, file_size)

        # PDF 预览生成 (无论有效性，和文本预览保持一致)
        pdf_preview_nodes = []
        if self.pdf_preview_pages > 0 and self._is_pdf_file(file_name):
            logger.info(f"[{group_id}] 📄 尝试生成 PDF 预览 ({self.pdf_preview_pages} 页)")
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

        if is_gfs_valid:
            # 文件有效，检查是否需要媒体转换（视频或图片）
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
            
            if self.notify_on_success:
                success_message = f"✅ 您发送的文件「{file_name}」初步检查有效。"
                if preview_text:
                    # 文件结构列表不截断，普通文本预览才截断
                    is_file_structure = preview_extra_info == "文件结构"
                    if is_file_structure:
                        preview_text_short = preview_text
                    else:
                        preview_text_short = preview_text[:self.preview_length]
                    
                    success_message += f"\n{preview_extra_info}，以下是预览：\n{preview_text_short}"
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
            elif pdf_preview_nodes:
                # 如果没开启成功通知但有 PDF 预览
                yield event.chain_result([Comp.Nodes(nodes=pdf_preview_nodes)])
                logger.info(f"[{group_id}] ✅ PDF 预览已发送 ({len(pdf_preview_nodes)} 页)")

            logger.info(f"[{group_id}] 初步检查通过，已加入延时复核队列。")
            asyncio.create_task(self._task_delayed_recheck(event, file_name, file_id, file_component, preview_text))
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
                    
                    failure_message += f"\n{preview_extra_info}，以下是预览：\n{preview_text_short}"
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
                        logger.info(f"[{group_id}] 补档完成，删除已失效的原文件")
                        # 重新查询文件ID以确保准确删除
                        current_file_id = await self._search_file_id_by_name(event, file_name)
                        if current_file_id:
                            await self._delete_group_file(event, current_file_id, file_name)
                        else:
                            logger.warning(f"[{group_id}] 无法查询到原文件ID，可能已被删除")
            except Exception as send_e:
                logger.error(f"[{group_id}] [阶段一] 回复失效通知时再次发生错误: {send_e}")

    async def _check_validity_via_gfs(self, event: AstrMessageEvent, file_id: str) -> bool:
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
                    logger.info(f"已清理临时文件夹: {extract_path}")
                except Exception as e:
                    logger.warning(f"删除临时文件夹 {extract_path} 失败: {e}")

    async def _get_preview_for_file(self, file_name: str, file_component: Comp.File, file_size: Optional[int] = None) -> tuple[str, str]:
        is_text = self._is_text_file(file_name)
        is_archive = self.enable_zip_preview and self._is_archive_file(file_name)
        
        if not (is_text or is_archive):
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
            if is_text:
                with open(local_file_path, 'rb') as f:
                    content_bytes = f.read(self.preview_length * 4)
                preview_text, encoding = self._get_preview_from_bytes(content_bytes)
                extra_info = f"格式为 {encoding}"
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

    async def _task_delayed_recheck(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, preview_text: str):
        """延时复核任务"""
        await asyncio.sleep(self.check_delay_seconds)
        group_id = int(event.get_group_id())
        
        logger.info(f"[{group_id}] [阶段二] 开始延时复核: '{file_name}'")
        
        is_still_valid = await self._check_validity_via_gfs(event, file_id)
        
        if not is_still_valid:
            logger.error(f"❌ [{group_id}] [阶段二] 文件 '{file_name}' 在延时复核时确认已失效!")
            try:
                failure_message = f"❌ 经 {self.check_delay_seconds} 秒后复核，您发送的文件「{file_name}」已失效。"
                await event.send(MessageChain([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(failure_message)]))
                
                # 只有在 file_component 不为 None 时才尝试补档
                if file_component and self.repack_file_extensions:
                    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
                    if file_ext in self.repack_file_extensions:
                        logger.info(f"文件在延时复核时失效，触发重新打包任务 (文件类型: {file_ext})...")
                        await self._repack_and_send_file(event, file_name, file_component)
                        # 补档后删除已失效的原文件
                        logger.info(f"[{group_id}] 补档完成，删除已失效的原文件")
                        # 重新查询文件ID以确保准确删除
                        current_file_id = await self._search_file_id_by_name(event, file_name)
                        if current_file_id:
                            await self._delete_group_file(event, current_file_id, file_name)
                        else:
                            logger.warning(f"[{group_id}] 无法查询到原文件ID，可能已被删除")
                elif not file_component:
                    logger.debug(f"[{group_id}] 该文件为补档后的文件，无法再次补档")

            except Exception as send_e:
                logger.error(f"[{group_id}] [阶段二] 回复失效通知时再次发生错误: {send_e}")
        else:
            logger.info(f"✅ [{group_id}] [阶段二] 文件 '{file_name}' 延时复核通过，保持沉默。")

    async def terminate(self):
        logger.info("插件 [群文件预览助手] 已卸载。")
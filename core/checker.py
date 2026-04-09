import os
import asyncio
import time
import subprocess
import re
from typing import List, Dict, Optional

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api import logger
import astrbot.api.message_components as Comp
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
from .utils import is_msg_still_available, react_to_msg, get_group_config, build_notification_text, backup_file_to_session


class CheckerManager:
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self.temp_dir = plugin_instance.temp_dir
        self.download_semaphore = plugin_instance.download_semaphore
        self.supported_archive_formats = plugin_instance.supported_archive_formats
        self.supported_text_formats = plugin_instance.supported_text_formats

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

    async def _repack_and_send_file(self, event: AstrMessageEvent, original_filename: str, file_component: Comp.File, repack_config: dict = None, local_path: Optional[str] = None):
        """重新打包失效文件（直接使用 local_path）"""
        if repack_config is None:
            repack_config = {}
        repack_zip_password = repack_config.get("repack_zip_password", "")

        base_name = os.path.basename(original_filename)
        if re.search(r'[\\/|*<>;"\x00-\x1F\x7F]', base_name):
            logger.error(f"文件名 '{original_filename}' 包含非安全字符，已跳过重新打包。")
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain("❌ 文件名包含不安全字符，已跳过重新打包。")])
            return

        if local_path is None or not os.path.exists(local_path):
            logger.warning(f"无可用本地文件，跳过重新打包: {original_filename}")
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain("❌ 无可用文件，无法重新打包。")])
            return

        repacked_file_path = None
        try:
            logger.info(f"开始为失效文件 {original_filename} 进行重新打包...")

            base_name = os.path.splitext(original_filename)[0]
            new_zip_name = f"{base_name}.zip"
            repacked_file_path = os.path.join(self.temp_dir, f"{int(time.time())}_{new_zip_name}")

            # local_path 已是原始文件名，zip -j 取 basename 正确
            command = ['zip', '-j', repacked_file_path, local_path]
            if repack_zip_password:
                command.extend(['-P', repack_zip_password])

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
                check_config = get_group_config(self.plugin.config, str(event.get_group_id()), "check_module")
                asyncio.create_task(self._task_delayed_recheck(event, new_zip_name, new_file_id, None, None, custom_msg_id=new_msg_id, upload_time=int(time.time()), check_config=check_config, repack_config=repack_config))
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

    async def _task_delayed_recheck(self, event: AstrMessageEvent, file_name: str, file_id: str, file_component: Comp.File, preview_text: str, custom_msg_id: str | None = None, upload_time: Optional[int] = None, check_config: dict = None, repack_config: dict = None, backup_config: dict = None, local_path: Optional[str] = None):
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
            logger.debug(f"[{group_id}] 目标消息 {target_msg_id} 已撤回，停止延时复核流程。")
            return

        logger.debug(f"[{group_id}] [阶段二] 开始延时复核: '{file_name}'")

        is_still_valid = await self._check_validity_via_gfs(event, file_id)

        if not is_still_valid:
            # 报告失效前再次检查原消息（或补档通知）是否还在
            if not await is_msg_still_available(event, target_msg_id):
                return

            enable_emoji = check_config.get("enable_emoji", True)
            await react_to_msg(event, "357", enable_emoji)
            logger.error(f"❌ [{group_id}] [阶段二] 文件 '{file_name}' 在延时复核时确认已失效!")
            try:
                failure_message = f"❌ 经 {check_delay_seconds} 秒后复核，您发送的文件「{file_name}」已失效。"
                await event.send(MessageChain([Comp.Reply(id=target_msg_id), Comp.Plain(failure_message)]))

                # 只有在 file_component 不为 None 时才尝试补档
                if file_component:
                    repack_extensions_str = repack_config.get("repack_file_extensions", "").strip()
                    repack_file_extensions = [ext.strip().lower() for ext in repack_extensions_str.split(",") if ext.strip()]

                    file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')

                    if not repack_extensions_str or file_ext in repack_file_extensions:
                        logger.info(f"文件在延时复核时失效，触发重新打包任务 (文件类型: {file_ext})...")
                        async for msg_chain in self._repack_and_send_file(event, file_name, file_component, repack_config, local_path=local_path):
                            await event.send(msg_chain)
                        # 补档后删除已失效的原文件
                        logger.info(f"[{group_id}] 补档完成，已创建 10 分钟后的延迟删除任务")
                        asyncio.create_task(self._delayed_delete_file(event, file_name, 600, upload_time))
                elif not file_component:
                    logger.debug(f"[{group_id}] 该文件为补档后的文件，无法再次补档")

                # 备份（仅备份失效文件模式，复核阶段也需要备份）
                if backup_config and backup_config.get("target_sid"):
                    await backup_file_to_session(self.plugin.context, file_name, backup_config, local_path)

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
        local_path: Optional[str] = None
    ):
        """处理失效文件的逻辑：贴表情、发送通知、触发补档"""

        enable_emoji = check_config.get("enable_emoji", True)

        if not await is_msg_still_available(event, event.message_obj.message_id):
            return
        await react_to_msg(event, "357", enable_emoji)

        # 构建失效通知文案
        preview_config = get_group_config(self.plugin.config, str(event.get_group_id()), "preview_module")
        failure_message = build_notification_text(
            file_name, False, preview_text, extra_info, preview_config
        )

        # 使用合并转发发送 PDF 预览（如果有）
        if pdf_preview_images:
            for msg in self.plugin.preview.send_pdf_preview(event, failure_message, pdf_preview_images):
                yield msg
        else:
            yield event.chain_result([Comp.Reply(id=event.message_obj.message_id), Comp.Plain(failure_message)])

        # 补档逻辑
        if file_component:
            repack_extensions_str = repack_config.get("repack_file_extensions", "").strip()
            repack_file_extensions = [ext.strip().lower() for ext in repack_extensions_str.split(",") if ext.strip()]
            file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')

            if not repack_extensions_str or file_ext in repack_file_extensions:
                logger.info(f"文件失效，触发重新打包任务 (文件类型: {file_ext if file_ext else '无后缀'})...")
                async for msg in self._repack_and_send_file(event, file_name, file_component, repack_config, local_path=local_path):
                    yield msg
                asyncio.create_task(self._delayed_delete_file(event, file_name, 600, upload_time))

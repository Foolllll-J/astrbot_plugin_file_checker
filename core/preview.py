import os
import asyncio
import zipfile
import xml.etree.ElementTree as ET
from typing import List, Optional
import time
import chardet
import subprocess
import re
import pypdfium2 as pdfium

from astrbot.api.event import AstrMessageEvent
from astrbot.api import logger
import astrbot.api.message_components as Comp


class PreviewManager:
    def __init__(self, plugin_instance):
        self.plugin = plugin_instance
        self.temp_dir = plugin_instance.temp_dir
        self.download_semaphore = plugin_instance.download_semaphore
        self.supported_archive_formats = plugin_instance.supported_archive_formats
        self.supported_text_formats = plugin_instance.supported_text_formats

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

    async def _convert_file_to_media(self, event: AstrMessageEvent, file_name: str, file_size: int, local_path: Optional[str], media_type: str):
        """将文件转换为媒体形式发送（支持视频和图片，仅使用已下载的 local_path）"""
        group_id = int(event.get_group_id())

        if local_path is None or not os.path.exists(local_path):
            logger.warning(f"[{group_id}] ⚠️ 无可用本地文件，跳过{media_type}转换: {file_name}")
            return

        try:
            media_name = "视频" if media_type == "video" else "图片"
            emoji = "🎬" if media_type == "video" else "🖼️"
            logger.info(f"[{group_id}] {emoji} 开始{media_name}转换流程: {file_name}")

            file_size_mb = file_size / (1024 * 1024)
            absolute_path = os.path.abspath(local_path)

            logger.debug(f"[{group_id}] 📤 准备以{media_name}形式发送文件 ({file_size_mb:.2f} MB): {absolute_path}")

            if media_type == "video":
                media_message = [Comp.Video(file=f"file:///{absolute_path}")]
            else:  # image
                media_message = [Comp.Image.fromFileSystem(absolute_path)]

            yield event.chain_result(media_message)

            logger.info(f"[{group_id}] ✅ {media_name}发送成功，将在 30 分钟后删除群文件")

            # 30分钟后删除群文件（本地文件由 main.py 统一清理）
            delete_delay = 1800  # 30分钟
            asyncio.create_task(self._delayed_cleanup_group_file_only(event, file_name, delete_delay))

            return  # 转换成功

        except Exception as e:
            media_name = "视频" if media_type == "video" else "图片"
            logger.error(f"[{group_id}] ❌ {media_name}发送失败: {e}", exc_info=True)
            return  # 发送失败

    async def _delayed_cleanup_group_file_only(self, event: AstrMessageEvent, file_name: str, delay: int):
        """延迟清理群文件（本地文件由 main.py 统一清理）"""
        await asyncio.sleep(delay)

        group_id = int(event.get_group_id())
        logger.debug(f"[{group_id}] 开始延迟清理群文件: {file_name}")

        # 通过文件名查询最新的 file_id
        file_id = await self.plugin.checker._search_file_id_by_name(event, file_name)

        if file_id:
            await self.plugin.checker._delete_group_file(event, file_id, file_name)
        else:
            logger.warning(f"[{group_id}] ⚠️ 无法查询到文件ID，可能文件已被删除或移动")

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

    async def _get_preview_from_archive(self, file_path: str, file_name: str, preview_config: dict = None, keep_extracted_pdf: bool = False) -> tuple[str, str]:
        """通用压缩包预览方法，支持多种格式"""
        if preview_config is None:
            preview_config = {}
        default_zip_password = preview_config.get("default_zip_password", "")
        preview_length = preview_config.get("preview_length", 500)

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
                error_message = stderr.decode('utf-8', errors='ignore')
                logger.error(f"无密码解压 {archive_type} 失败 (返回码 {process.returncode}): {error_message}")
                if default_zip_password:
                    logger.info(f"无密码解压 {archive_type} 失败，正在尝试使用默认密码...")
                    command_with_pwd = ["7za", "x", file_path, f"-o{extract_path}", f"-p{default_zip_password}", "-y"]
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

            if not all_extracted_files:
                return "", f"{archive_type} 为空或解压失败"

            # 查找所有支持预览的文件（文本文件 + EPUB + PDF）
            text_files = [f for f in all_extracted_files
                            if any(f.lower().endswith(ext) for ext in self.supported_text_formats)]
            epub_files = [f for f in all_extracted_files if f.lower().endswith('.epub')]
            pdf_files = [f for f in all_extracted_files if f.lower().endswith('.pdf')]
            previewable_files = text_files + epub_files + pdf_files

            # 先输出文件结构（无论是否预览）
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

            # 判断条件：只有单个可预览文件，且压缩包总共也只有这一个文件
            if len(previewable_files) == 1 and len(all_extracted_files) == 1:
                # 处理单个可预览文件
                target_file = previewable_files[0]
                safe_text_name = os.path.basename(target_file)

                if re.search(r'[\\/|*<>;"\x00-\x1F\x7F]', safe_text_name):
                    logger.error(f"解压出的文件名 '{safe_text_name}' 包含非安全字符，跳过预览。")
                    return "", "解压出的文件名不安全"

                # 判断文件类型并处理
                if target_file.lower().endswith('.pdf'):
                    # PDF 需要返回文件路径供外层生成图片预览
                    # 复制 PDF 到临时目录根目录，避免被 finally 清理
                    import shutil
                    pdf_temp_path = os.path.join(self.temp_dir, f"temp_pdf_{int(time.time())}.pdf")
                    try:
                        shutil.copy2(target_file, pdf_temp_path)
                    except Exception as e:
                        logger.error(f"复制 PDF 文件失败: {e}", exc_info=True)
                        return "", "PDF 文件处理失败"
                    # 格式：PDF_PATH:<实际路径>，extra_info 也包含解压信息
                    return f"PDF_PATH:{pdf_temp_path}", f"已解压「{safe_text_name}」"
                elif target_file.lower().endswith('.epub'):
                    preview_text = self._extract_epub_text(target_file, preview_length)
                    extra_info = f"已解压「{safe_text_name}」"
                else:
                    with open(target_file, 'rb') as f:
                        content_bytes = f.read(preview_length * 4)
                    preview_text, encoding = self._get_preview_from_bytes(content_bytes)
                    extra_info = f"已解压「{safe_text_name}」(格式 {encoding})"

                return preview_text, extra_info

            # 不满足预览条件，输出压缩包的文件结构
            structure_text = "\n".join(file_structure)
            return structure_text, "文件结构"
            
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
                    logger.debug(f"已清理压缩包解压临时目录: {extract_path}")
                except Exception as e:
                    logger.warning(f"删除临时文件夹 {extract_path} 失败: {e}")

    def _is_epub_file(self, file_name: str) -> bool:
        """检查文件是否为 EPUB 格式"""
        return file_name.lower().endswith('.epub')

    def _extract_epub_text(self, epub_path: str, max_chars: int = 1000) -> str:
        """从 EPUB 文件中提取纯文本预览内容"""
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

    def _should_download_for_preview(self, file_name: str, file_size: Optional[int], preview_config: dict = None) -> bool:
        """判断是否需要为预览下载文件"""
        if preview_config is None:
            preview_config = {}
        preview_length = preview_config.get("preview_length", 500)
        zip_extraction_size_limit_mb = preview_config.get("zip_extraction_size_limit_mb", 0)

        if preview_length <= 0:
            return False

        is_text = self._is_text_file(file_name)
        is_epub = self._is_epub_file(file_name)
        # zip_extraction_size_limit_mb > 0 时才启用压缩包预览
        is_archive = zip_extraction_size_limit_mb > 0 and self._is_archive_file(file_name)

        if not (is_text or is_epub or is_archive):
            return False

        # 压缩包大小检查
        if is_archive and file_size is not None:
            archive_size_mb = file_size / (1024 * 1024)
            if archive_size_mb > zip_extraction_size_limit_mb:
                return False

        return True

    async def _get_preview_for_file(self, file_name: str, local_path: Optional[str], file_size: Optional[int] = None, preview_config: dict = None) -> tuple[str, str]:
        """获取文件预览（仅使用已下载的 local_path）
        
        Args:
            local_path: 已下载的文件路径。如果为 None 或不存在，返回空预览。
            
        Returns:
            (preview_text, extra_info)
        """
        if preview_config is None:
            preview_config = {}
        preview_length = preview_config.get("preview_length", 500)

        if preview_length <= 0:
            return "", ""

        if local_path is None or not os.path.exists(local_path):
            return "", ""

        is_text = self._is_text_file(file_name)
        is_epub = self._is_epub_file(file_name)
        is_archive = self._is_archive_file(file_name)

        if is_epub:
            preview_text = self._extract_epub_text(local_path, preview_length)
            return preview_text, "📖 EPUB 内容预览"

        if is_text:
            with open(local_path, 'rb') as f:
                content_bytes = f.read(preview_length * 4)
            preview_text, encoding = self._get_preview_from_bytes(content_bytes)
            extra_info = f"📄 文本预览 ({encoding})"
            return preview_text, extra_info

        if is_archive:
            preview_text, extra_info = await self._get_preview_from_archive(local_path, file_name, preview_config)
            return preview_text, extra_info

        return "", ""

    async def _get_pdf_preview(self, file_path: str, preview_config: dict = None) -> List[str]:
        """使用 pypdfium2 生成 PDF 预览图"""
        if preview_config is None:
            preview_config = {}
        pdf_preview_pages = preview_config.get("pdf_preview_pages", 0)
        
        image_paths = []
        try:
            pdf = pdfium.PdfDocument(file_path)
            num_pages = len(pdf)
            pages_to_render = min(num_pages, pdf_preview_pages)
            
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

    def send_pdf_preview(self, event: AstrMessageEvent, message_text: str, pdf_preview_images: List[str]):
        """使用合并转发发送 PDF 预览图"""
        if not pdf_preview_images:
            return

        message_text += f"\n📄 PDF 预览图如下："
        sender_id = event.get_self_id()
        group_id = int(event.get_group_id())

        pdf_preview_nodes = [Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Plain(message_text)])]
        for img in pdf_preview_images:
            pdf_preview_nodes.append(Comp.Node(uin=sender_id, name="PDF 预览", content=[Comp.Image.fromFileSystem(img)]))

        yield event.chain_result([Comp.Nodes(nodes=pdf_preview_nodes)])
        logger.info(f"[{group_id}] ✅ PDF 预览已发送 ({len(pdf_preview_images)} 页，包含文字通知)")

        # 清理临时图
        for img in pdf_preview_images:
            if os.path.exists(img):
                os.remove(img)

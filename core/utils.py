import logging
import re
import os
from astrbot.api.event import AstrMessageEvent
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
import astrbot.api.message_components as Comp

logger = logging.getLogger("astrbot")

async def is_msg_still_available(event: AstrMessageEvent, msg_id: str) -> bool:
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

async def react_to_msg(event: AstrMessageEvent, emoji_id: str, enable_emoji: bool):
    """贴表情回应（仅支持 aiocqhttp）"""
    if not enable_emoji or not isinstance(event, AiocqhttpMessageEvent):
        return
    try:
        await event.bot.api.call_action(
            "set_msg_emoji_like",
            message_id=int(event.message_obj.message_id),
            emoji_id=int(emoji_id)
        )
    except Exception as e:
        logger.warning(f"[FileChecker] 贴表情回应失败 (emoji_id={emoji_id}): {e}")

def get_group_config(config: dict, group_id: str, module_name: str) -> dict:
    """
    从 template_list 配置中获取特定群的配置。
    如果找不到特定群的配置，则返回全局配置（group_id 为空列表或空字符串）。
    如果都找不到，返回空字典。
    """
    module_config = config.get(module_name, [])
    if not isinstance(module_config, list):
        return {}
        
    # 1. 尝试寻找特定群的配置
    for item in module_config:
        target_group_ids = item.get("group_id", [])
        if isinstance(target_group_ids, str):
            target_group_ids = [target_group_ids] if target_group_ids else []
        
        if str(group_id) in [str(gid) for gid in target_group_ids]:
            return item
            
    # 2. 尝试寻找全局配置
    for item in module_config:
        target_group_ids = item.get("group_id", [])
        if not target_group_ids:
            return item

    return {}

def find_file_component(event: AstrMessageEvent):
    """从消息中找到文件组件"""
    for segment in event.get_messages():
        if isinstance(segment, Comp.File):
            return segment
    return None

def purify_file_name(file_name: str, rules: list) -> str:
    """按正则规则净化文件名"""
    if not rules or not isinstance(rules, list):
        return file_name

    result = file_name
    for pattern in rules:
        if not pattern or not isinstance(pattern, str):
            continue

        try:
            result = re.sub(pattern, "", result)
        except re.error as e:
            logger.warning(f"[FileChecker] 文件名净化规则无效: pattern={pattern}, error={e}")

    return result

async def backup_file_to_session(context, file_name: str, backup_config: dict, local_path: str) -> bool:
    """将文件备份到目标会话"""
    if not backup_config or not isinstance(backup_config, dict):
        return False

    target_sid = backup_config.get("target_sid", "").strip()
    if not target_sid:
        return False

    backup_extensions = backup_config.get("backup_extensions", "").strip()
    if backup_extensions:
        ext_list = [ext.strip().lower() for ext in backup_extensions.split(",") if ext.strip()]
        file_ext = os.path.splitext(file_name)[1].lower().lstrip('.')
        if file_ext not in ext_list:
            return False

    if local_path is None or not os.path.exists(local_path):
        logger.warning(f"[FileChecker] 备份失败：无可用本地文件 {file_name}")
        return False

    try:
        from astrbot.api.event import MessageChain
        import astrbot.api.message_components as Comp
        chain = MessageChain(chain=[Comp.File(name=file_name, file=os.path.abspath(local_path))])
        await context.send_message(target_sid, chain)
        logger.info(f"[FileChecker] 文件已备份到会话 {target_sid}: {file_name}")
        return True
    except Exception as e:
        logger.error(f"[FileChecker] 备份失败: {e}")
        return False

def build_notification_text(
    file_name: str,
    is_success: bool,
    preview_text: str = "",
    extra_info: str = "",
    preview_config: dict = None
) -> str:
    """构建通知文案"""
    if preview_config is None:
        preview_config = {}
    preview_length = preview_config.get("preview_length", 500)

    if is_success:
        base_msg = f"✅ 您发送的文件「{file_name}」初步检查有效。"
    else:
        base_msg = f"⚠️ 您发送的文件「{file_name}」已失效。"

    # 如果有 extra_info，优先显示 extra_info
    if extra_info:
        if preview_text:
            # 文件结构列表不截断，普通文本预览才截断
            is_file_structure = extra_info == "文件结构"
            if is_file_structure:
                preview_text_short = preview_text
            else:
                preview_text_short = preview_text[:preview_length]

            base_msg += f"\n{extra_info}：\n{preview_text_short}"
            if not is_file_structure and len(preview_text) > preview_length:
                base_msg += "..."
        else:
            # 只有 extra_info 没有 preview_text（如压缩包内 PDF）
            base_msg += f"\n{extra_info}"

    return base_msg

<div align="center">

# 🔍 QQ 文件预览

<i>📡 多格式秒开预览，全天候失效预警</i>

![License](https://img.shields.io/badge/license-AGPL--3.0-green?style=flat-square)
![Python](https://img.shields.io/badge/python-3.10+-blue?style=flat-square&logo=python&logoColor=white)
![AstrBot](https://img.shields.io/badge/framework-AstrBot-ff6b6b?style=flat-square)

</div>

## 📖 简介

一款为 [AstrBot](https://astrbot.app) 设计的、功能强大的文件检查与预览插件。它可以精准检查文件是否有效，并能立即提供内容预览（支持文本、EPUB、压缩包、PDF等），在一段时间后对有效文件进行二次复核，以确保文件的可用性。

---

## ✨ 功能

* **🛡️ 全生命周期失效管理**
  采用“即时预检 + 延时复核”双重机制。复核通过静默不打扰，确认失效则发送通知；支持**自动补档**功能，将失效文件加密打包为 ZIP 重新发送，确保文件永久可用。
* **👁️ 多格式即时预览**
  * **文档**: 支持 **PDF** (预览图)、**EPUB** (智能文本提取) 及 **30+种代码/文本** (自动编码识别)。
  * **压缩包**: 支持 ZIP/7Z/TAR 等多种格式解压，可预览文件结构或内部文本内容。
* **🎬 媒体文件优化**
  自动检测并转换视频 (MP4) 和图片 (JPG/PNG/GIF/WEBP) 为媒体消息发送，无需下载即可查看，并自动清理源文件。
* **♻️ 重复文件检测**
  自动识别群内历史文件，避免重复上传，并提供历史记录引用。

---

## 🚀 安装

1. **进入容器安装依赖**：如果你的 AstrBot 运行在 Docker 容器中，请先进入容器内部。
   
   ```bash
   docker ps  # 找到你的 AstrBot 容器ID
   docker exec -it <容器ID> bash  # 或者 sh
   ```
   
   然后在容器内安装 `zip` 依赖库：
   
   ```bash
   # 对于 Debian/Ubuntu 系统
   apt-get update && apt-get install zip p7zip-full
   # 对于 Alpine Linux 系统
   apk add zip p7zip
   ```
2. 下载本仓库。
3. 将整个 `astrbot_plugin_file_checker` 文件夹放入 `astrbot` 的 `plugins` 目录中。
4. 重启 AstrBot。

---

## ⚙️ 配置

首次加载后，请在 AstrBot 后台 -> 插件 页面找到本插件进行设置。所有配置项都有详细的说明和提示。

---

## 💡 使用

本插件采用两阶段工作模式：

#### 阶段一：即时预览与诊断

当白名单内的群聊有成员发送文件时，插件会在等待“预检延时”后，立即进行一次检查。

* **如果文件已存在**: 插件将直接回复已有的历史记录，并结束流程。
* **如果文件在群文件中有效**:
  * 机器人会发送一条成功通知。
  * 如果文件是文本、EPUB、压缩包（ZIP/7Z 等）或 PDF，通知中还会附带**内容预览**、**文件结构**或**PDF预览图**（合并转发形式）。
  * 同时，此文件会被加入**延时复核队列**。
* **如果文件在群文件中失效**:
  * 机器人会发送一条**警告**，并根据配置尝试补档。

#### 阶段二：延时复核

* 只有在**第一阶段完全成功**的文件，或者**被重新打包的新文件**，才会进入此阶段。
* 等待“复核延时”结束后，插件会再次通过**群文件系统API**检查该文件。
* 如果此时文件**依然有效**，机器人会**保持沉默**，不发送任何消息。
* 如果此时文件**已经失效**，机器人会**再次引用回复**原始消息，并发送“经xx秒后复核，已失效”的通知。

---

## 📝 更新日志

详见 [CHANGELOG](CHANGELOG.md)

---

## ❓ 常见问题

### **Q: 插件突然不工作了，或者启动时报错，我该怎么办？**

**A:** 在遇到任何插件运行错误或功能异常时，可按照以下步骤尝试解决：

1. **重载插件**
2. **重启 AstrBot**
3. **重启 NapCat**

---

## ❤️ 支持

* [AstrBot 帮助文档](https://astrbot.app)
* 如果您在使用中遇到问题，欢迎在本仓库提交 [Issue](https://github.com/Foolllll-J/astrbot_plugin_file_checker/issues)。

---

<div align="center">

**觉得好用的话，给个 ⭐ Star 吧！**

</div>


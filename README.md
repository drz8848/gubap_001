#
股吧爬虫 
#
确保 MySQL 服务已启动，且已创建 guba_db 数据库：
#
登录 MySQL
#
mysql -u root -p
#
输入密码 13579sh
#
创建数据库
#
CREATE DATABASE IF NOT EXISTS guba_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
#
退出
#
EXIT;
#
安装爬虫依赖（若未安装）：
#
pip install requests==2.28.2 requests-html==0.10.0 beautifulsoup4==4.12.3 lxml==4.9.3 fake_useragent==0.1.11 pymysql==1.0.3 lxml[html_clean] -i https://pypi.tuna.tsinghua.edu.cn/simple
#
按目录结构创建文件，进入项目根目录运行：
#
cd guba
#
python crawlStockPostMutilThread.py

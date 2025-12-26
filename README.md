股吧爬虫/n
确保 MySQL 服务已启动，且已创建 guba_db 数据库：/n
登录 MySQL/n
mysql -u root -p/n
输入密码 13579sh/n
创建数据库/n
CREATE DATABASE IF NOT EXISTS guba_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;/n
退出/n
EXIT;/n
安装爬虫依赖（若未安装）：/n
pip install requests==2.28.2 requests-html==0.10.0 beautifulsoup4==4.12.3 lxml==4.9.3 fake_useragent==0.1.11 pymysql==1.0.3 lxml[html_clean] -i https://pypi.tuna.tsinghua.edu.cn/simple/n
按目录结构创建文件，进入项目根目录运行：/n
cd guba/n
python crawlStockPostMutilThread.py/n

#! /bin/bash
#--------------------------------------------
# 这是一个从101.201.55.199上下载数据的shell脚本
#
# 功能：从mysql数据库中导出为txt文件
# 特色：
#--------------------------------------------
##########################################################
#######读取配置文件
##########################################################
#shell脚本当前目录
shell_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#conf目录的路径
conf=${shell_dir%/*}/conf
#读取配置文件
source ${conf}/g01-shell.cfg
##########################################################
######路径配置
##########################################################

##########################################################
######程序运行
##########################################################
#运行mysql语句
mysql -u${mysql_local_user} -p${mysql_local_passworld} -h${mysql_local_ip} -e "
use ${mysql_local_database}
########################################################
################清空tbc_dic_web_title_region表数据
########################################################
truncate table tbc_dic_web_title_region;
################向tbc_dic_web_title_region插入省份不为空的数据
REPLACE INTO tbc_dic_web_title_region (
	url,
	title,
	province,
	city,
	county,
	organization
) SELECT DISTINCT
	a.url,
	a.title,
	b.province,
	a.city,
	a.county,
	a.organization
FROM
	tbc_dic_web_title_tmp a
INNER JOIN city b ON a.province = b.province where a.organization IS NOT NULL and a.organization!='';
################向tb_dic_web_title_region插入市不为空的数据，并根据市补全省份信息
REPLACE INTO tbc_dic_web_title_region (
	url,
	title,
	province,
	city,
	county,
	organization
) SELECT DISTINCT
	a.url,
	a.title,
	b.province,
	b.city,
	a.county,
	a.organization
FROM
	tbc_dic_web_title_tmp a
INNER JOIN city b ON a.city = b.city
WHERE
	a.city IS NOT NULL and a.organization IS NOT NULL and a.organization!='';
################向tb_dic_web_title_region插入县（区）不为空的数据，并根据县（区）补全省、市信息
REPLACE INTO tbc_dic_web_title_region (
	url,
	title,
	province,
	city,
	county,
	organization
) SELECT
	a.url,
	a.title,
	b.province,
	b.city,
	b.county,
	a.organization
FROM
	tbc_dic_web_title_tmp a
INNER JOIN city b ON a.county = b.county
WHERE
	a.county IS NOT NULL and a.organization IS NOT NULL and a.organization!='';
########################################################
################清空tbc_dic_web_title_region_null表数据
########################################################
truncate table tbc_dic_web_title_region_null;
#####################将未直接匹配省、市、县成功的数据插入到tbc_dic_web_title_region_null中
INSERT INTO tbc_dic_web_title_region_null SELECT
	url,
	title,
	province,
	city,
	county,
	organization
FROM
	tbc_dic_web_title_tmp
WHERE
	url NOT IN (
		SELECT
			url
		FROM
			tbc_dic_web_title_region
	);
########################################################
################清空tbc_dic_web_title_region2表数据
########################################################
truncate table tbc_dic_web_title_region2;
################将模糊匹配省成功的数据导入到tbc_dic_web_title_region2中
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	NULL AS city,
	NULL AS county,
	SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.province, - 1),
		\"网\",
		1
	)
FROM
	tbc_dic_web_title_region_null a
LEFT JOIN (
	SELECT DISTINCT
		CASE SUBSTR(province, - 1)
	WHEN \"省\" THEN
		SUBSTRING_INDEX(province, \"省\", 1)
	WHEN \"市\" THEN
		SUBSTRING_INDEX(province, \"市\", 1)
	ELSE
		SUBSTRING_INDEX(province, \"区\", 1)
	END AS province
	FROM
		city
)b ON instr(a.title, b.province) > 0
LEFT JOIN city c ON instr(c.province, b.province) > 0
WHERE
	b.province IS NOT NULL and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.province, - 1),
		\"网\",
		1
	) IS NOT NULL and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.province, - 1),
		\"网\",
		1
	)='';
################将模糊匹配市成功的数据导入到tbc_dic_web_title_region2中，并根据市补全省份信息
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	c.city,
	NULL AS county,
	SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.city, - 1),
		\"网\",
		1
	)
FROM
	tbc_dic_web_title_region_null a
LEFT JOIN (
	SELECT DISTINCT
		SUBSTRING_INDEX(province, \"省\", 1) AS province,
		SUBSTRING_INDEX(city, \"市\", 1) AS city
	FROM
		city
)b ON instr(a.title, b.city) > 0
LEFT JOIN city c ON instr(c.province, b.province) > 0
AND instr(c.city, b.city) > 0
WHERE
	b.city IS NOT NULL and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.city, - 1),
		\"网\",
		1
	) is not null
	and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.city, - 1),
		\"网\",
		1
	) !='';
################将模糊匹配县（区）成功的数据导入到tbc_dic_web_title_region2中，并根据县（区）补全省份信息
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	c.city,
	c.county,
	SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.county, - 1),
		\"网\",
		1
	)
FROM
	tbc_dic_web_title_region_null a
LEFT JOIN (
	SELECT DISTINCT
		SUBSTRING_INDEX(province, \"省\", 1) AS province,
		SUBSTRING_INDEX(city, \"市\", 1) AS city,
		CASE SUBSTR(county, - 1)
	WHEN \"市\" THEN
		SUBSTRING_INDEX(county, \"市\", 1)
	WHEN \"县\" THEN
		SUBSTRING_INDEX(county, \"县\", 1)
	ELSE
		SUBSTRING_INDEX(county, \"区\", 1)
	END AS county
	FROM
		city
	WHERE
		LENGTH(county) >= 9
) b ON instr(a.title, b.county) > 0
LEFT JOIN city c ON instr(c.province, b.province) > 0
AND instr(c.city, b.city) > 0
AND instr(c.county, b.county) > 0
WHERE
	b.county IS NOT NULL and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.county, - 1),
		\"网\",
		1
	) is not null
	and SUBSTRING_INDEX(
		SUBSTRING_INDEX(a.title, b.county, - 1),
		\"网\",
		1
	) != '';
########################################################
################清空tbc_dic_web_title_region_null2表数据
########################################################
truncate table tbc_dic_web_title_region_null2;
#####################向tbc_dic_web_title_region_null2中导入第二次模糊匹配省、市、县未成功的数据
INSERT INTO tbc_dic_web_title_region_null2
SELECT
	*
FROM
	tbc_dic_web_title_region_null
WHERE
	url NOT IN (
		SELECT
			url
		FROM
			tbc_dic_web_title_region2
	);
########################################################
################合并两次匹配成功的结果，导入tbc_dic_web_region中
########################################################
	REPLACE INTO tbc_dic_web_region (
	url,
	province,
	city,
	county,
	organization
) SELECT DISTINCT
	a.url,
	a.province,
	a.city,
	a.county,
	b.organization
FROM
	(
		SELECT
			*
		FROM
			tbc_dic_web_title_region
		UNION ALL
			SELECT
				*
			FROM
				tbc_dic_web_title_region2
	) a
LEFT JOIN tbc_dic_web_artificial_success b ON a.url = b.url
and b.organization is not null
and b.organization!= '';
################去除单位字段中的特殊字符
UPDATE tbc_dic_web_region
SET organization = REPLACE (organization,\"！\", \"\"),
 organization = REPLACE (organization, \"首页\", \"\"),
 organization = REPLACE (organization, \">>\", \"\"),
 organization = REPLACE (organization, \"::\", \"\"),
 organization = REPLACE (organization, \"主页\", \"\"),
 organization = REPLACE (organization, \"-\", \"\"),
 organization = REPLACE (organization, \"|\", \"\"),
 organization = REPLACE (organization, \"top.asp\", \"\"),
 organization = REPLACE (organization, \">\", \"\"),
 organization = REPLACE (organization, \"  \", \"\"),
 organization = REPLACE (organization, \"－\", \"\"),
 organization = REPLACE (organization, \"_\", \"\"),
 organization = REPLACE (organization, \",\", \"\"),
 organization = REPLACE (organization, \"官方\", \"\"),
 organization = REPLACE (organization, \"【\", \"\"),
 organization = REPLACE (organization, \"】\", \"\"),
 organization = REPLACE (organization, \"・\", \"\"),
 organization = REPLACE (organization, \"/\", \"\"),
 organization = REPLACE (organization, \"（\", \"\"),
 organization = REPLACE (organization, \"）\", \"\"),
 organization = REPLACE (organization, \"―\", \"\"),
 organization = REPLACE (organization, \"欢迎您\", \"\"),
 organization = REPLACE (organization, \"丨\", \"\"),
 organization = REPLACE (organization, \"	\", \"\"),
 organization = REPLACE (organization, \"网\", \"\");
########################################################
################将手工也无法获取到的数据加入黑名单
########################################################
REPLACE INTO tbc_dic_web_artificial_fail
SELECT
	url
FROM
	tbc_dic_web_artificial
WHERE
	url NOT IN (
		SELECT
			url
		FROM
			tbc_dic_web_region
	);
########################################################
################对比tbc_dic_web_region数据，删除已获取到信息的黑名单数据
########################################################
DELETE
FROM
	tbc_dic_web_artificial_fail
WHERE
	url IN (
		SELECT
			url
		FROM
			tbc_dic_web_region
	);
"



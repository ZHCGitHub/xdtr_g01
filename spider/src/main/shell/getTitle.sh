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
delete from tbc_dic_web_title_region;
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
	'',
	'',
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
	'',
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
delete from tbc_dic_web_title_region_null;
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
delete from tbc_dic_web_title_region2;
################将模糊匹配省成功的数据导入到tbc_dic_web_title_region2中
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	\"\" AS city,
	\"\" AS county,
	a.organization
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
) b ON instr(a.title, b.province) > 0
LEFT JOIN city c ON instr(c.province, b.province) > 0
WHERE
	b.province IS NOT NULL
#AND SUBSTRING_INDEX(
#	SUBSTRING_INDEX(a.title, b.province, - 1),
#	\"网\",
#	1
#) IS NOT NULL
#AND SUBSTRING_INDEX(
#	SUBSTRING_INDEX(a.title, b.province, - 1),
#	\"网\",
#	1
#) != ''
;
################将模糊匹配市成功的数据导入到tbc_dic_web_title_region2中，并根据市补全省份信息
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	c.city,
	\"\" AS county,
	a.organization
FROM
	tbc_dic_web_title_region_null a
LEFT JOIN (
	SELECT DISTINCT
		SUBSTRING_INDEX(province, \"省\", 1) AS province,
		SUBSTRING_INDEX(city, \"市\", 1) AS city
	FROM
		city
) b ON instr(a.title, b.city) > 0
LEFT JOIN city c ON instr(c.province, b.province) > 0
AND instr(c.city, b.city) > 0
WHERE
	b.city IS NOT NULL
#AND SUBSTRING_INDEX(
#	SUBSTRING_INDEX(a.title, b.city, - 1),
#	\"网\",
#	1
#) IS NOT NULL
#AND SUBSTRING_INDEX(
#	SUBSTRING_INDEX(a.title, b.city, - 1),
#	\"网\",
#	1
#) != ''
;
################将模糊匹配县（区）成功的数据导入到tbc_dic_web_title_region2中，并根据县（区）补全省份信息
REPLACE INTO tbc_dic_web_title_region2 SELECT DISTINCT
	a.url,
	a.title,
	c.province,
	c.city,
	c.county,
	a.organization
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
	b.county IS NOT NULL
#  AND SUBSTRING_INDEX(
# 	SUBSTRING_INDEX(a.title, b.county, - 1),
# 	\"网\",
# 	1
#  ) IS NOT NULL
#  AND SUBSTRING_INDEX(
# 	SUBSTRING_INDEX(a.title, b.county, - 1),
# 	\"网\",
# 	1
#  ) != ''
;
########################################################
################清空tbc_dic_web_title_region_null2表数据
########################################################
delete from tbc_dic_web_title_region_null2;
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
##################################################################################################################################
#####################合并直接匹配成功和模糊匹配成功的数据到tbc_dic_web_region中
##################################################################################################################################
################向tbc_dic_web_region中插入两次切分到省、市、县并且url不在梁哥获取到的数据中数据
INSERT INTO tbc_dic_web_region (
	url,
	province,
	city,
	county,
	organization
) SELECT
	url,
	province,
	city,
	county,
	organization
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
	) AS T
WHERE
	url NOT IN (
		SELECT DISTINCT
			url
		FROM
			(
				SELECT
					*
				FROM
					(
						SELECT
							a.url,
							a.website,
							REPLACE (
								a.realDeptName,
								'人民政府办公室',
								'人民政府'
							) realDeptName,
							a.deptName,
							a.flag_mark,
							a.flag_num
						FROM
							v_dic_department a
					) a
				LEFT JOIN tb_dic_result_gov b ON a.realDeptName = b.company
			) a
		WHERE
			a.cat IS NOT NULL
		ORDER BY
			a.realDeptName
	) AND url NOT IN (SELECT DISTINCT url FROM tbc_dic_web_region);
################插入梁哥根据黄页获取到的网站省、市、县数据（以梁哥的为主）
REPLACE INTO tbc_dic_web_region (
	url,
	province,
	city,
	county,
	organization
) SELECT
	url,
	province,
	city,
	county,
	company
FROM
	(
		SELECT
			*
		FROM
			(
				SELECT
					a.url,
					a.website,
					REPLACE (
						a.realDeptName,
						'人民政府办公室',
						'人民政府'
					) realDeptName,
					a.deptName,
					a.flag_mark,
					a.flag_num
				FROM
					v_dic_department a
			) a
		LEFT JOIN tb_dic_result_gov b ON a.realDeptName = b.company
	) a
WHERE
	a.cat IS NOT NULL
ORDER BY
	a.realDeptName;
################去除单位字段中的特殊字符
UPDATE tbc_dic_web_region
SET organization = REPLACE (organization,\"！\", \"\"),
 organization = REPLACE (organization, \"本站\", \"\"),
 organization = REPLACE (organization, \"门户网站群\", \"\"),
 organization = REPLACE (organization, \"首页\", \"\"),
 organization = REPLACE (organization, \"浏览\", \"\"),
 organization = REPLACE (organization, \"网站\", \"\"),
 organization = REPLACE (organization, \"官网\", \"\"),
 organization = REPLACE (organization, \"欢迎进入\", \"\"),
 organization = REPLACE (organization, \"欢迎来到\", \"\"),
 organization = REPLACE (organization, \"欢迎光临\", \"\"),
 organization = REPLACE (organization, \"欢迎使用\", \"\"),
 organization = REPLACE (organization, \"对不起，受限\", \"\"),
 organization = REPLACE (organization, \" \", \"\"),
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
 organization = REPLACE (organization,\"访问\", \"\"),
 organization = REPLACE (organization,\"Powered by PageAdmin CMS\", \"\"),
 organization = REPLACE (organization,\"无标题文档\", \"\"),
 organization = REPLACE (organization,\"...\", \"\"),
 organization = REPLACE (organization,\"河南高考信息\", \"\"),
 organization = REPLACE (organization,\"Powered By SiteEngine\", \"\"),
 organization = REPLACE (organization,\"热线\", \"\"),
 organization = REPLACE (organization,\"Powered by CmsEasy\", \"\"),
 organization = REPLACE (organization,\"powered by shubiao\", \"\"),
 organization = REPLACE (organization,\"!\", \"\"),
 organization = REPLACE (organization,\"门户\", \"\"),
 organization = REPLACE (organization,\" Powered by Discuz!\", \"\"),
 organization = REPLACE (organization,\"在线\", \"\"),
 organization = REPLACE (organization,\"index\", \"\"),
 organization = REPLACE (organization,\"..\", \"\"),
 organization = REPLACE (organization,\"——\", \"\")
;
########################################################
################向tbc_dic_web_region中的depart_level字段打上是否政府单位标示（0，政府单位；1，其他单位）
################替换tbc_dic_web_region中的null为空字符串
########################################################
UPDATE tbc_dic_web_region
SET depart_level = (
	CASE
	WHEN organization LIKE \"%政府%\" THEN
		0
	WHEN organization LIKE \"%局%\" THEN
		0
	WHEN organization LIKE \"%办公室%\" THEN
		0
	WHEN organization LIKE \"%委员会%\" THEN
		0
	WHEN organization LIKE \"%中心%\" THEN
		0
	WHEN organization LIKE \"%厅%\" THEN
		0
	WHEN organization LIKE \"%所%\" THEN
		0
	WHEN organization LIKE \"%法院%\" THEN
		0
	WHEN organization LIKE \"%学院%\" THEN
		0
	WHEN organization LIKE \"%校%\" THEN
		0
	WHEN organization LIKE \"%学%\" THEN
		0
	WHEN organization LIKE \"%教育%\" THEN
		0
	WHEN organization LIKE \"%教研%\" THEN
		0
	WHEN organization LIKE \"%组织部%\" THEN
		0
	WHEN organization LIKE \"%电视台%\" THEN
		0
	WHEN organization LIKE \"%频道%\" THEN
		0
	WHEN organization LIKE \"%支队%\" THEN
		0
	WHEN organization LIKE \"%医院%\" THEN
		0
	WHEN organization LIKE \"%检察院%\" THEN
		0
	ELSE
		1
	END
),
 province = (
	CASE
	WHEN province IS NULL THEN
		\"\"
	ELSE
		province
	END
),
 city = (
	CASE
	WHEN city IS NULL THEN
		\"\"
	ELSE
		city
	END
),
 county = (
	CASE
	WHEN county IS NULL THEN
		\"\"
	ELSE
		county
	END
);
########################################################
################清空tbc_dic_web_artificial表数据
########################################################
delete from tbc_dic_web_artificial;
################向tbc_dic_web_artificial表中导入需要手工处理的url
INSERT INTO tbc_dic_web_artificial
SELECT
	url
FROM
	tbc_dic_url_count
WHERE
	url NOT IN (
		SELECT
			url
		FROM
			tbc_dic_web_region
	);
"



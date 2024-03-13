# ---------------------------------------------------------------------------
# 定时任务配置字符串解析
# ---------------------------------------------------------------------------
#
class CronFiled(object):
    """
    格式: "秒 分 时 日 月 周 年"  or "秒 分 时 日 月 周"
    ————————————————
    各个字段的含义如下:
        字段              允许值                                    允许的特殊字符
    秒(Seconds)          0~59的整数                          , - * /    四个字符
    分(Minutes)          0~59的整数                          , - * /    四个字符
    小时(Hours)          0~23的整数                          , - * /    四个字符
    日期(DayofMonth)     1~31的整数(但是你需要考虑你月的天数)     ,- * ? / L W C     八个字符
    月份(Month)          1~12的整数或者 JAN-DEC               , - * /    四个字符
    星期(DayofWeek)      1~7的整数或者 SUN-SAT (1=SUN)        , - * ? / L C #     八个字符
    年(可选，留空)(Year)   1970~2099                          , - * /    四个字符
    ————————————————
    每一个域可出现的字符如下:
    Seconds(秒):可出现", - * /"四个字符，有效范围为0-59的整数
    Minutes(分):可出现", - * /"四个字符，有效范围为0-59的整数
    Hours(时):可出现", - * /"四个字符，有效范围为0-23的整数
    DayofMonth(日):可出现", - * / ? L C W"八个字符，有效范围为0-31的整数
    Month(月):可出现", - * /"四个字符，有效范围为1-12的整数或JAN-DEC
    DayofWeek(周):可出现", - * / ? L C #"四个字符，有效范围为1-7的整数或SUN-SAT两个范围。1表示星期天，2表示星期一， 依次类推
    Year(年):可出现", - * /"四个字符，有效范围为1970-2099年
    ————————————————
    每一个域都使用数字，但还可以出现如下特殊字符，它们的含义是：
        *  表示匹配该域的任意值，假如在Minutes域使用*, 即表示每分钟都会触发事件。
        ?  只能用在DayofMonth和DayofWeek两个域。它也匹配域的任意值，但实际不会。因为DayofMonth和DayofWeek会相互影响。
           例如想在每月的20日触发调度，不管20日到底是星期几，则只能使用如下写法： 13 13 15 20 * ?, 其中最后一位只能用?，
           而不能使用*，如果使用*表示不管星期几都会触发，实际上并不是这样。
           所以当指定了日期(DayofMonth)后(包括每天*)，星期(DayofWeek)必须使用问号(?)，
           同理，指定星期(DayofWeek)后，日期(DayofMonth)必须使用问号(?)。
        -  表示范围，例如在Minutes域使用5-20，表示从5分到20分钟每分钟触发一次 。
        /  表示起始时间开始触发，然后每隔固定时间触发一次，例如在Minutes域使用5/20,则意味着5分钟触发一次，而25，45等分别触发一次。
        ,  表示列出枚举值值。例如：在Minutes域使用5,20，则意味着在5和20分每分钟触发一次。
        L  表示最后，只能出现在DayofWeek和DayofMonth域，如果在DayofWeek域使用5L,意味着在每月的最后一个星期四触发。
        W  表示有效工作日(周一到周五),只能出现在DayofMonth域，系统将在离指定日期的最近的有效工作日触发事件。
           例如：在 DayofMonth使用5W，如果5日是星期六，则将在最近的工作日：星期五，即4日触发。
           如果5日是星期天，则在6日(周一)触发；如果5日在星期一到星期五中的一天，则就在5日触发。另外一点，W的最近寻找不会跨过月份。
        LW 这两个字符可以连用，表示在某个月最后一个工作日，即最后一个星期五。
        #  用于确定每个月第几个星期几，只能出现在DayofMonth域。例如在4#2，表示某月的第二个星期三。
    ————————————————
    """

    def __init__(self, cronStr):
        """

        """
        self.cronStr = cronStr
        self.cron = {}
        self._getCronFiled()
        self._getCronDesc()

    def _getCronFiled(self):
        """
        格式: "秒 分 时 日 月 周 年"  or "秒 分 时 日 月 周"
        """
        cronList = self.cronStr.split(' ')
        cronLen = len(cronList)

        if cronLen not in [6, 7]:
            raise ValueError('Wrong number of fields; got {}, expected 6 or 7'.format(cronLen))

        self.cron['timezone'] = 'Asia/Shanghai'
        self.cron['second'] = cronList[0]
        self.cron['minute'] = cronList[1]
        self.cron['hour'] = cronList[2]
        self.cron['day'] = cronList[3]
        self.cron['month'] = cronList[4]
        self.cron['day_of_week'] = cronList[5]
        if cronLen == 7:
            self.cron['year'] = cronList[6]
        else:
            self.cron['year'] = '*'

    def _getCronDesc(self):
        """

        """
        keyDesc = {'year': '年', 'day_of_week': '周', 'month': '月',
                   'day': '日', 'hour': '小时', 'minute': '分钟', 'second': '秒'}
        weekDesc = {'0': '周日', '1': '周一', '2': '周二', '3': '周三', '4': '周四', '5': '周五', '6': '周六'}

        self.cronDesc = ''
        for key in keyDesc:
            cronValue = self.cron[key]
            descSuffix = keyDesc[key]
            if cronValue in ['*', '?']:
                continue

            desc = ''
            for i in cronValue.split(','):
                if '-' in i:
                    s = f'从第{i.split("-")[0]}{descSuffix}到第{i.split("-")[1]}{descSuffix}之间'
                elif '/' in i:
                    s = f'{i.split("/")[0]}{descSuffix}/每间隔{i.split("/")[1]}{descSuffix}'
                else:
                    s = f'{i}{descSuffix}'

                desc = desc + s

            self.cronDesc = self.cronDesc + desc

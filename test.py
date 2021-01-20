import datetime
import uuid

todayDate = datetime.datetime.today().strftime('%Y-%m-%d')

nextWeekFixturesJsonKey = ''.join(['raw-data/', str(uuid.uuid4().hex[:6]),
                                    "-nextWeekFixturesJson-", todayDate])

row = {'idFixture':"sfd", 'status':"kjh", 'date':"jkh",
                'hour':'kl', 'idHomeTeam':"hkj", 'idAwayTeam':'mljk'}


print(row)

import win32com.client
import datetime
import pandas as pd
import xlsxwriter as xl
import numpy as np
from tabulate import tabulate
from plyer import notification

path = # Set Target Path (include final "/" , it will prevent issues with file output command

# Email function
def email_out(to_address, subject, body, attachment):
    outlook = win32com.client.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)
    mail.To = to_address
    mail.Subject = subject
    mail.Body = body
    mail.Attachments.Add(path + attachment)
    mail.Send()

# Variables
today = datetime.datetime.today() + datetime.timedelta(days=1)
end = today + datetime.timedelta(days=1)
start = today.replace(hour=0, minute=0,second=0).strftime('%m/%d/%Y %H:%M %p')
end = end.replace(hour=0, minute=0,second=0).strftime('%m/%d/%Y %H:%M %p')
from_address = # ENTER OUTLOOK EMAIL ADDRESS
to_address = # ENTER TO EMAIL ADDRESS
subject = today.strftime('%m/%d/%Y') + ' Meeting Schedule'
TableHeader = [str(today.strftime('%m/%d/%Y')) + ' Meetings', 'Start', 'Duration(Minutes)']
TableBody = []

# Outlook Connection
outlook = win32com.client.Dispatch('outlook.application')
mapi = outlook.GetNamespace('MAPI')
appointments = mapi.Folders(from_address).Folders(22).Items
appointments.IncludeRecurrences = True
appointments.Sort("[Start]")
restrict = "[Start] >= '" + start + "' AND [Start] <= '" + end +"'"
appointments = appointments.Restrict(restrict)

# Compile Results
for apt in list(appointments):
    meeting = []
    meeting.append(apt.Subject)
    meeting.append(apt.Start.Format('%H:%M %p'))
    meeting.append(apt.Duration)
    TableBody.append(meeting)
print(tabulate(TableBody, headers=TableHeader))
rows, cols = np.array(TableBody).shape

# ########## Optional File Output 
# df = pd.DataFrame(TableBody)
# df.to_csv(path + 'Daily Schedule.csv')
#
# ########## Deliver Email
# body = 'Schedule Attached'
# # email_out(to_address, subject, body, 'Daily Schedule.xlsx')
#
########## Windows Notification
notification.notify(title=str(today.strftime('%m/%d/%Y')) + ' Meeting Schedule Exported',
                    message= "Tomorrow's schedule has been emailed\n"
                             "You have " + str(rows -1 ) + " meetings tomorrow")


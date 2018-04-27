import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''

cur.execute(statement)

statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''

cur.execute(statement)

statement = '''
            CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL,
            'Area' REAL 
            );
            '''

cur.execute(statement)

f = open(COUNTRIESJSON, "r")
countries = f.read()
countries_obj = json.loads(countries)
f.close()

for data in countries_obj:
    insertion = (data["alpha2Code"], data["alpha3Code"], data["name"], data["region"], data["subregion"], data["population"], data["area"])
    statement = 'INSERT INTO Countries (Alpha2, Alpha3, EnglishName, Region, Subregion, Population, Area)'
    statement += 'VALUES(?,?,?,?,?,?,?)'
    cur.execute(statement, insertion)
    

statement = '''
            CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY,
            'Company' TEXT NOT NULL,
            'SpecificBeanBarName' INTEGER NOT NULL,
            'REF' TEXT NOT NULL,
            'ReviewDate' TEXT NOT NULL,
            'CocoaPercent' REAL NOT NULL,
            'CompanyLocation' TEXT NOT NULL,
            'CompanyLocationId' INTEGER,
            'Rating' REAL NOT NULL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT NOT NULL,
            'BroadBeanOriginId' INTEGER 
            );
            '''

cur.execute(statement)

f = open(BARSCSV, "r")
bars = csv.reader(f)
next(bars)

for row in bars:
    companyLocId = cur.execute("SELECT Id FROM Countries WHERE EnglishName =? ", (row[5],)).fetchone()[0]
    if row[8] == "Unknown":
        beanOrgId = "none"
    else:
        beanOrgId = cur.execute("SELECT Id FROM Countries WHERE EnglishName =? ", (row[8],)).fetchone()[0]

    insertion = (row[0] , row[1], row[2], row[3], row[4], row[5], companyLocId ,row[6], row[7], row[8], beanOrgId)
    statement = 'INSERT INTO Bars (Company, SpecificBeanBarName, REF, ReviewDate, CocoaPercent, CompanyLocation, CompanyLocationId ,Rating, BeanType, BroadBeanOrigin, BroadBeanOriginId)'
    statement += 'VALUES (?,?,?,?,?,?,?,?,?,?,?)'  
    cur.execute(statement, insertion)

f.close()
conn.commit()
conn.close()

# Part 2: Implement logic to process user commands
def process_command(command):
    conn = sqlite3.connect(DBNAME)
    cur = conn.cursor()

    cmd_list = command.lower().split()
    cmd = cmd_list[0]
    params = cmd_list[1:]
    params_dic = {
                    "sort":"rating", 
                    "order":"top", 
                    "limit":10, 
                    "type_filter_for_bar":"", 
                    "type_filter_for_other":"", 
                    "filter_value":"" , 
                    "country_filter":"sellers"
                }

    sort_list = ["ratings" , "cocoa" , "bars_sold"]
    order_list = ["top", "bottom"]
    type_filter_for_bar = ["sellcountry", "sourcecountry", "sellregion", "sourceregion"]
    type_filter_for_other = ["country", "region"]
    country_filter = ["sellers" , "sources"]

    input_valid = True

    for param in params:
        if param in sort_list:
            params_dic["sort"] = param
        elif param in country_filter:
            params_dic["country_filter"] = param
        elif "=" in param:
            param_list = param.split("=")
            if param_list[0] in order_list and len(param_list)== 2:
                params_dic["order"] = param_list[0]
                params_dic["limit"] = param_list[1]
            elif param_list[0] in type_filter_for_bar and len(param_list)==2:
                params_dic["type_filter_for_bar"] = param_list[0]
                params_dic["filter_value"] = param_list[1]
            elif param_list[0] in type_filter_for_other and len(param_list)==2:
                params_dic["type_filter_for_other"] = param_list[0]
                params_dic["filter_value"] = param_list[1]
        else:
            input_valid = False
            return None


    if input_valid == True:
        if cmd == "bars":
            statement = "SELECT b.SpecificBeanBarName, b.Company, b.CompanyLocation, b.rating, b.CocoaPercent, b.BroadBeanOrigin "
            statement += "FROM Bars AS b "
            if params_dic["type_filter_for_bar"] == "sellcountry" or params_dic["type_filter_for_bar"] == "sellregion":
                statement += "JOIN Countries AS c ON b.CompanyLocationId = c.Id "
                if params_dic["type_filter_for_bar"] == "sellcountry":
                    insertion = (params_dic["filter_value"].upper(), params_dic["limit"])
                    statement += "WHERE c.Alpha2 =? "
                else:
                    insertion = (params_dic["filter_value"].title(), params_dic["limit"])
                    statement += "WHERE c.Region =? "
            elif params_dic["type_filter_for_bar"] == "sourcecountry" or params_dic["type_filter_for_bar"] == "sourceregion":
                statement += "JOIN Countries AS c ON b.BroadBeanOriginId = c.Id "               
                if params_dic["type_filter_for_bar"] == "sourcecountry": 
                    insertion = (params_dic["filter_value"].upper(), params_dic["limit"])
                    statement += "WHERE c.Alpha2 =? "
                else:
                    insertion = (params_dic["filter_value"].title(), params_dic["limit"]) 
                    statement += "WHERE c.Region =? "
            else:
                insertion = (params_dic["limit"],)
            statement += "ORDER BY "
            if params_dic["sort"] == "ratings":
                statement += "b.Rating "
            else:
                statement += "b.CocoaPercent*100 "  
            if params_dic["order"] == "top":
                statement += "DESC "
            statement += "LIMIT ? "

        elif cmd == "companies":
            statement = "SELECT b.Company, b.CompanyLocation, "
            if params_dic["sort"] == "ratings":
                statement += "ROUND(AVG(b.Rating),1) "
            elif params_dic["sort"] == "cocoa":
                statement += "ROUND(AVG(b.CocoaPercent),2) "
            else: 
                statement += "COUNT(*) "

            statement += "FROM Bars AS b "
            if params_dic["type_filter_for_other"] == "country":
                statement += "JOIN Countries AS c ON b.CompanyLocationId = c.Id WHERE c.Alpha2=? "
                insertion = (params_dic["filter_value"].upper(), params_dic["limit"])
            elif params_dic["type_filter_for_other"] == "region": 
                statement += "JOIN Countries AS c ON b.CompanyLocationId = c.Id WHERE c.Region=? "
                insertion = (params_dic["filter_value"].title(), params_dic["limit"])
            else:
                insertion = (params_dic["limit"])

            statement += "GROUP BY b.Company HAVING COUNT(*)>4 ORDER BY "
            if params_dic["sort"] == "ratings":
                statement += "AVG(b.Rating) "
            elif params_dic["sort"] == "cocoa":
                statement += "AVG(b.CocoaPercent*100) "
            else: 
                statement += "COUNT(*) "
            if params_dic["order"] == "top":
                statement += "DESC "
            statement += "LIMIT ? "

        elif cmd == "countries":
            statement = "SELECT c.EnglishName, c.Region, "
            if params_dic["sort"] == "ratings":
                statement += "ROUND(AVG(b.Rating),1) "
            elif params_dic["sort"] == "cocoa":
                statement += "ROUND(AVG(b.CocoaPercent),2) "
            else: 
                statement += "COUNT(*) "
            statement += "FROM Bars AS b JOIN Countries AS c "
            if params_dic["country_filter"] == "sellers":
                statement += "ON b.CompanyLocationId = c.Id "
            else:
                statement += "ON b.BroadBeanOriginId = c.Id "
            if params_dic["type_filter_for_other"] == "region":
                statement += "WHERE c.Region=?"
                insertion = (params_dic["filter_value"].title(), params_dic["limit"])
            else:
                insertion = (params_dic["limit"], )
            statement += "GROUP BY c.EnglishName HAVING COUNT(*) >4 ORDER BY "
            if params_dic["sort"] == "ratings":
                statement += "AVG(b.Rating) "
            elif params_dic["sort"] == "cocoa":
                statement += "AVG(b.CocoaPercent*100) "
            else: 
                statement += "COUNT(*) "
            if params_dic["order"] == "top":
                statement += "DESC "
            statement += "LIMIT ? "
        elif cmd == "regions":
            insertion = (params_dic["limit"],)
            statement = "SELECT c.Region, "
            if params_dic["sort"] == "ratings":
                statement += "ROUND(AVG(b.Rating),1) "
            elif params_dic["sort"] == "cocoa":
                statement += "ROUND(AVG(b.CocoaPercent),2) "
            else: 
                statement += "COUNT(*) "

            statement += "FROM Bars AS b JOIN Countries AS c "
            if params_dic["country_filter"] == "sellers":
                statement += "ON b.CompanyLocationId = c.Id "
            else:
                statement += "ON b.BroadBeanOriginId = c.Id "
            statement += "GROUP BY c.Region HAVING COUNT(*) >4 ORDER BY "
            if params_dic["sort"] == "ratings":
                statement += "AVG(b.Rating) "
            elif params_dic["sort"] == "cocoa":
                statement += "AVG(b.CocoaPercent*100) "
            else: 
                statement += "COUNT(*) "
            if params_dic["order"] == "top":
                statement += "DESC "
            statement += "LIMIT ? "
        else:
            return None

        return_list = cur.execute(statement, insertion).fetchall()
        conn.commit()
        conn.close()
        return return_list
    


def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue
        elif response == 'exit':
            break
        elif response == '':
            print("Command not recognized:" + response)
            continue
        else:
            res_list = process_command(response)
            if res_list == None:
                print("Command not recognized:" + response)
                continue
            else:
                for row in res_list:
                    format_str = ""
                    for data in row:
                        if type(data) == type("data"):
                            if '%' in data:
                                format_data = "{:5}".format(data)
                            else:
                                format_data = "{:16}".format(processStrData(data))
                        elif type(data) ==  type(0):
                            format_data = "{:<5}".format(data)
                        elif type(data) == type(1.0):
                            format_data = "{:<5.1f}".format(data)
                        format_str += format_data
                    print(format_str)

def processStrData(str):
    if len(str)>12:
        new_str = str[:12] + "..."
    else:
        new_str = str
    return new_str



# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()

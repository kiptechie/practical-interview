import json
import psycopg2

class DatabaseConnection:
    def __init__(self):
        try:

            self.connection = psycopg2.connect(dbname='practicalinterview',
                                               user='postgres',
                                               host='localhost',
                                               password='')

            self.cursor = self.connection.cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print("\n\n\Error!!! : {}\n\n\n".format(error))


    def drop_table(self):
      drop_table_command= '''DROP TABLE IF EXISTS clients'''
      self.cursor.execute(drop_table_command)
      self.connection.commit()

    def create_table(self):
        create_table_command = '''CREATE TABLE IF NOT EXISTS clients(
                                    ticketID serial PRIMARY KEY,
                                    clientName VARCHAR (200) NOT NULL,
                                    mobileNo VARCHAR (200) NOT NULL,
                                    contactType VARCHAR (200) NOT NULL,
                                    callType VARCHAR (200) NOT NULL,
                                    sourceName VARCHAR (200) NOT NULL,
                                    storeName VARCHAR (200) NOT NULL,
                                    questionType VARCHAR (200) NOT NULL,
                                    questionSubType VARCHAR (200) NOT NULL,
                                    dispositionName VARCHAR (200) NOT NULL,
                                    dateCreated VARCHAR (200) NOT NULL
                                    )'''
        self.cursor.execute(create_table_command)
        self.connection.commit()

    def insert_new_method(self):
      with open('app/interview_201909.json') as json_file:
        data = json.load(json_file)
        print(data[1])

        for client in data[1]['Report']:
            new_record = (client['ticketID'], client['clientName'], client['mobileNo'],
                          client['contactType'], client['callType'], client['sourceName'],
                          client['storeName'], client['questionType'], client['questionSubType'],
                          client['dispositionName'], client['dateCreated'])
            print(new_record)

            insert_command = "INSERT INTO clients(ticketID, clientName, mobileNo, contactType, callType, sourceName, storeName, questionType, questionSubType, dispositionName, dateCreated) VALUES('" + \
                new_record[0] + "', '" + new_record[1] + "', '" + \
                new_record[2] + "', '" + new_record[3] + "','" + \
                new_record[4] + "', '"+new_record[5] + "', '"+ \
                new_record[6] + "', '"+new_record[7] + "', '"+ \
                new_record[8] + "', '"+new_record[9] + "', '"+ \
                new_record[10] + "')"


            self.cursor.execute(insert_command)


            self.connection.commit()


if __name__ == '__main__':
    database_connection = DatabaseConnection()
    database_connection.drop_table()
    database_connection.create_table()
    database_connection.insert_new_method()

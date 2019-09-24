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
        drop_table_command = '''DROP TABLE IF EXISTS table1 CASCADE'''
        drop_table_command2 = '''DROP TABLE IF EXISTS table2'''
        drop_table_command3 = '''DROP TABLE IF EXISTS table3'''
        drop_table_command4 = '''DROP TABLE IF EXISTS table4'''
        self.cursor.execute(drop_table_command)
        self.cursor.execute(drop_table_command2)
        self.cursor.execute(drop_table_command3)
        self.cursor.execute(drop_table_command4)

        self.connection.commit()

    def create_table(self):
        create_table1_command = '''CREATE TABLE IF NOT EXISTS table1(
                                    ticketID INTEGER,
                                    clientName VARCHAR (200) NOT NULL,
                                    mobileNo VARCHAR (200) NOT NULL,
                                    dateCreated VARCHAR (200) NOT NULL,
                                    CONSTRAINT PK PRIMARY KEY (ticketID)
                                    )'''

        self.cursor.execute(create_table1_command)

        create_table2_command = '''CREATE TABLE IF NOT EXISTS table2(
                                  ticketID INTEGER references table1(ticketID),
                                  contactType VARCHAR (200) NOT NULL,
                                  callType VARCHAR (200) NOT NULL,
                                  sourceName VARCHAR (200) NOT NULL,
                                  storeName VARCHAR (200) NOT NULL,
                                  dispositionName VARCHAR (200) NOT NULL
                                  )'''
        self.cursor.execute(create_table2_command)

        create_table3_command = '''CREATE TABLE IF NOT EXISTS table3(
                                  ticketID INTEGER references table1(ticketID),
                                  questionType VARCHAR (200) NOT NULL,
                                  questionSubType VARCHAR (200) NOT NULL
                                  )'''
        self.cursor.execute(create_table3_command)

        create_table4_command = '''CREATE TABLE IF NOT EXISTS table4(
                                  ticketID INTEGER references table1(ticketID),

                                  )'''
        self.cursor.execute(create_table4_command)

        self.connection.commit()

    def insert_new_method(self):
        with open('app/interview_201909.json') as json_file:
            data = json.load(json_file)

            for client in data[1]['Report']:
                new_record = (client['ticketID'], client['clientName'], client['mobileNo'],
                              client['contactType'], client['callType'], client['sourceName'],
                              client['storeName'], client['questionType'], client['questionSubType'],
                              client['dispositionName'], client['dateCreated'])

                insert_command = "INSERT INTO table1(ticketID, clientName, mobileNo, dateCreated) VALUES('" + \
                    new_record[0] + "', '" + new_record[1] + "', '" + \
                    new_record[2] + "', '" + new_record[10] + "')"
                self.cursor.execute(insert_command)

                insert_command2 = "INSERT INTO table2(ticketID, contactType, callType, sourceName, storeName, dispositionName) VALUES('" + \
                    new_record[0] + "', '" + new_record[3] + "','" + new_record[4] + "', '"+new_record[5] + "', '" + \
                    new_record[8] + "', '"+new_record[9] + "')"
                self.cursor.execute(insert_command2)

                insert_command3 = "INSERT INTO table3(ticketID,  questionType, questionSubType) VALUES('" + \
                    new_record[0] + "', '" + new_record[6] + \
                    "','" + new_record[7] + "')"
                self.cursor.execute(insert_command3)

                self.connection.commit()


if __name__ == '__main__':
    database_connection = DatabaseConnection()
    database_connection.drop_table()
    database_connection.create_table()
    database_connection.insert_new_method()

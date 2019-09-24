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
        drop_client_table = '''DROP TABLE IF EXISTS client CASCADE'''
        drop_table_command2 = '''DROP TABLE IF EXISTS contact'''
        drop_table_command3 = '''DROP TABLE IF EXISTS question'''
        self.cursor.execute(drop_client_table)
        self.cursor.execute(drop_table_command2)
        self.cursor.execute(drop_table_command3)

        self.connection.commit()

    def create_table(self):
        create_client_table = '''CREATE TABLE IF NOT EXISTS client(
                                    ticketID INTEGER,
                                    clientID INT,
                                    clientName VARCHAR (200) NOT NULL,
                                    mobileNo VARCHAR (200) NOT NULL,
                                    dateCreated VARCHAR (200) NOT NULL,
                                    CONSTRAINT PK PRIMARY KEY (ticketID)
                                    )'''
        self.cursor.execute(create_client_table)

        create_contact_table = '''CREATE TABLE IF NOT EXISTS contact(
                                  ticketID INTEGER references client(ticketID),
                                  contactType VARCHAR (200) NOT NULL,
                                  callType VARCHAR (200) NOT NULL,
                                  sourceName VARCHAR (200) NOT NULL,
                                  storeName VARCHAR (200) NOT NULL,
                                  dispositionName VARCHAR (200) NOT NULL
                                  )'''
        self.cursor.execute(create_contact_table)

        create_question_table = '''CREATE TABLE IF NOT EXISTS question(
                                  ticketID INTEGER references client(ticketID),
                                  questionType VARCHAR (200) NOT NULL,
                                  questionSubType VARCHAR (200) NOT NULL
                                  )'''
        self.cursor.execute(create_question_table)


        self.connection.commit()

    def insert_new_method(self):
        with open('app/interview_201909.json') as json_file:
            data = json.load(json_file)

            for client in data[1]['Report']:
                new_record = (client['ticketID'], client['clientName'], client['mobileNo'],
                              client['contactType'], client['callType'], client['sourceName'],
                              client['storeName'], client['questionType'], client['questionSubType'],
                              client['dispositionName'], client['dateCreated'])

                insert_clients = "INSERT INTO client(ticketID, clientName, mobileNo, dateCreated) VALUES('" + \
                    new_record[0] + "', '" + new_record[1] + "', '" + \
                    new_record[2] + "', '" + new_record[10] + "')"
                self.cursor.execute(insert_clients)

                insert_contacts = "INSERT INTO contact(ticketID, contactType, callType, sourceName, storeName, dispositionName) VALUES('" + \
                    new_record[0] + "', '" + new_record[3] + "','" + new_record[4] + "', '"+new_record[5] + "', '" + \
                    new_record[8] + "', '"+new_record[9] + "')"
                self.cursor.execute(insert_contacts)

                insert_question = "INSERT INTO question(ticketID,  questionType, questionSubType) VALUES('" + \
                    new_record[0] + "', '" + new_record[6] + \
                    "','" + new_record[7] + "')"
                self.cursor.execute(insert_question)

                self.connection.commit()


if __name__ == '__main__':
    database_connection = DatabaseConnection()
    database_connection.drop_table()
    database_connection.create_table()
    database_connection.insert_new_method()

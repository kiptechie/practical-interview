import json
import psycopg2


clients = '''
  {
    "Report": [
      {
       "ticketID": "1104",
        "clientName": "Harlan Maldonado",
        "mobileNo": "16466550032",
        "contactType": "Contact",
        "callType": "Others",
        "sourceName": "Email",
        "storeName": "KSRT_Sarit Center",
        "questionType": "General Inquiry",
        "questionSubType": "Career Inquiry",
        "dispositionName": "Compliment",
        "dateCreated": "2019-09-13 17:48:56"
      },
      {
        "ticketID": "1103",
        "clientName": "Troy Stafford",
        "mobileNo": "19723200285",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Email",
        "storeName": "KRIV_2 Rivers",
        "questionType": "General Inquiry",
        "questionSubType": "Supplier Inquiry",
        "dispositionName": "Supplier Inquiry",
        "dateCreated": "2019-09-13 17:44:13"
      },
      {
        "ticketID": "1102",
        "clientName": "Rooney Kemp",
        "mobileNo": "18356441216",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KSRT_Sarit Center",
        "questionType": "Items left on the counter",
        "questionSubType": "Items left on the counter ",
        "dispositionName": "Items Left on The Counter ",
        "dateCreated": "2019-09-13 17:28:14"
      },
      {
        "ticketID": "1101",
        "clientName": "Matthew Whitaker",
        "mobileNo": "19901603609",
        "contactType": "Non contact",
        "callType": "Others",
        "sourceName": "Inbound",
        "storeName": "KRIV_2 Rivers",
        "questionType": "General Inquiry",
        "questionSubType": "Career Inquiry",
        "dispositionName": "Disconnected ",
        "dateCreated": "2019-09-13 17:23:12"
      },
      {
        "ticketID": "1100",
        "clientName": "Seth Arnold",
        "mobileNo": "18115156982",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Product Availability (Not under promotion)",
        "dateCreated": "2019-09-13 17:15:36"
      },
      {
        "ticketID": "1099",
        "clientName": "Xavier Wyatt",
        "mobileNo": "12974287996",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Inbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Product Availability (Not under promotion)",
        "dateCreated": "2019-09-13 17:11:05"
      },
      {
        "ticketID": "1098",
        "clientName": "Dieter Copeland",
        "mobileNo": "18279582516",
        "contactType": "Non contact",
        "callType": "Others",
        "sourceName": "Inbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Disconnected ",
        "dateCreated": "2019-09-13 17:09:27"
      },
      {
        "ticketID": "1097",
        "clientName": "Arden Strong",
        "mobileNo": "18493220454",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Price Inquiry (Not under promotion)",
        "dateCreated": "2019-09-13 17:03:54"
      },
      {
        "ticketID": "1096",
        "clientName": "Nasim Gardner",
        "mobileNo": "12697788534",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Inbound",
        "storeName": "KHUB_Kenya",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Under Promotions)",
        "dispositionName": "Product availability (Under promotion)",
        "dateCreated": "2019-09-13 16:43:17"
      },
      {
        "ticketID": "1095",
        "clientName": "Henry Solis",
        "mobileNo": "14237739683",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Product Availability (Not under promotion)",
        "dateCreated": "2019-09-13 16:41:33"
      },
      {
        "ticketID": "1094",
        "clientName": "Theodore Le",
        "mobileNo": "18218715660",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KHUB_Kenya",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Under Promotions)",
        "dispositionName": "Product availability (Under promotion)",
        "dateCreated": "2019-09-13 16:36:48"
      },
      {
        "ticketID": "1093",
        "clientName": "Owen Gibbs",
        "mobileNo": "14779309205",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KHUB_Kenya",
        "questionType": "Product",
        "questionSubType": "Product Specification",
        "dispositionName": "Product availability (Under promotion)",
        "dateCreated": "2019-09-13 16:32:24"
      },
      {
        "ticketID": "1092",
        "clientName": "Kareem Turner",
        "mobileNo": "14865722945",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KHUB_Kenya",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Under Promotions)",
        "dispositionName": "Product availability (Under promotion)",
        "dateCreated": "2019-09-13 16:29:14"
      },
      {
        "ticketID": "1091",
        "clientName": "Matthew Vega",
        "mobileNo": "11065170155",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Inbound",
        "storeName": "KGLR_Galleria Shopping Mall",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Not Under Promotion)",
        "dispositionName": "Product Availability (Not under promotion)",
        "dateCreated": "2019-09-13 16:25:47"
      },
      {
        "ticketID": "1090",
        "clientName": "Keith Copeland",
        "mobileNo": "11051995370",
        "contactType": "Contact",
        "callType": "Inquiry",
        "sourceName": "Outbound",
        "storeName": "KHUB_Kenya",
        "questionType": "Product",
        "questionSubType": "Product Availabilty (Under Promotions)",
        "dispositionName": "Product availability (Under promotion)",
        "dateCreated": "2019-09-13 16:23:05"
      }
    ]
  }
'''

# data = json.loads(clients)

# for client in data['Report']:
#   client


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
      data = json.loads(clients)

      for client in data['Report']:
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

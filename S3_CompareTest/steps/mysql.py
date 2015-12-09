__author__ = 'rsantamaria'

import MySQLdb as mdb
import sys

# mdb.connect().close()

def connect(host, username, password, database) :
    return mdb.connect(host, username, password, database)

def close_connection(con) :
    con.close

def get_DB_version(con) :
    cur = con.cursor()
    cur.execute("SELECT VERSION()")
    ver = cur.fetchone()
    return ver

def select(con, query) :
    cur = con.cursor()
    cur.execute(query)
    result = cur.fetchall()
    return result

def execute(con, statement) :
    cur = con.cursor()
    cur.execute(statement)
    con.commit()


dev_mart = connect('xmart.dataagg-dev.moveaws.com', 'dataagg', 'UWm6WggX', 'xmart')


# if __name__ == "__main__":

    # cc = mdb.Connect('xmart.dataagg-dev.moveaws.com', 'dataagg', 'UWm6WggX', 'xmart')

    # print(get_DB_version(cc))

    # print(str(select(dev_mart, "select sourceagentid from xmart.agent nolock where sourceagentid = 'agent_test';")))

    # stg_list = select(dev_mart, "select * from stg_listing")
    # for line in stg_list:
    #     print line[1]
    # print(type(stg_list))

################ GETS THE NUMBER OF ROWS

    # x = str(select(cc, "select COUNT(*) from stg_listing_attribute_value"))
    #
    # first_range = x.find("(")
    # sec_range = x.find("L")
    #
    # print(int(x[first_range +2 : sec_range]))

################ EXECUTES DELETE STATEMENT.. CAN ALSO DO SP'S

    # rr = "21202652"
    # ss = "21210009"
    # quer = "delete from xmart.load_exception where sourcelistingid in (%s, %s);" % (rr, ss)

    # test = execute(cc, quer)

    #print(execute(cc, "insert into listing values( 10300, 200, 'Hello World')"))

    # close_connection(cc)

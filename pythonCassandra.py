from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement



def cassandra_process(*krgs):
    KEYSPACE = "testkeyspace"
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.execute("DROP KEYSPACE " + KEYSPACE)


    session.execute("""
        CREATE KEYSPACE %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
        """ % KEYSPACE)


    session.set_keyspace(KEYSPACE)
    session.execute("""
        CREATE TABLE mytable (
            thekey text,
            lastname text, 
            age text, 
            city text, 
            email text, 
            firstname text,
            PRIMARY KEY (thekey)
        )
        """)

#query = SimpleStatement("""
 #      INSERT INTO mytable (thekey,lastname, age , city, email, firstname)
  #     VALUES (%(key)s, %(lastname)s, %(age)s, %(city)s, %(email)s, %(firstname)s)
   #    """, consistency_level=ConsistencyLevel.ONE)


    prepared = session.prepare("""
        INSERT INTO mytable (thekey, lastname, age , city, email, firstname)
        VALUES (?, ?, ?, ?, ?, ?)
        """)

    for i in range(1):
        #session.execute(query, dict(key="key%d" % i, lastname='Jones', age='35' , city='Austin', email='shuvamoy@cts.com', firstname='shuvamoy'))
        session.execute(prepared, (str(i), 'Jones', '35','Austin','shuvamoy@cts.com','shuvamoy'))
        session.execute(prepared, (str(i+1), 'James', '20', 'Austin', 'somenath@cts.com', 'somenath'))
        future = session.execute("select * from mytable ")
        for i in future:
            print(i)


#session.execute(""" insert into users (lastname, age, city, email, firstname) values ('Jones', 35, 'Austin', 'bob@example.com', 'Bob') """)

#result = session.execute("select * from users where lastname='Jones' ")[0]
#print(result.lastname)
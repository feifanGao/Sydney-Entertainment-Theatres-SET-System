#!/usr/bin/env python3
from re import S
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y21s1c9120_unikey"
    passwd = "passwd"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"
    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate a sales agent login request based on username and password
'''
def checkUserCredentials(userName, password):
    # TODO - validate and get user info for a sales agent

    conn = openConnection()
    with conn.cursor() as cursor:
        query = """
            SELECT agentid, username, firstname, lastname,password
            FROM AGENT
            WHERE username = %s AND password = %s
        """
        cursor.execute(query, (userName,password,))
        userdata = cursor.fetchone()
        if userdata is None:
            return None
        userdata = list(userdata)

    conn.close()

    return userdata


'''
List all the associated bookings in the database for a given sales agent Id
'''
def findBookingsBySalesAgent(agentId):
    # TODO - list all the associated bookings in DB for a given sales agent Id

    conn = openConnection()
    with conn.cursor() as cursor:
        query = """
                SELECT BOOKING.booking_no,CONCAT(CUSTOMER.firstname,' ',CUSTOMER.lastname),BOOKING.performance,BOOKING.performance_date,CONCAT(AGENT.firstname,' ',AGENT.lastname),BOOKING.instruction
                FROM BOOKING 
                JOIN CUSTOMER ON BOOKING.customer = CUSTOMER.email
                LEFT JOIN AGENT ON BOOKING.booked_by = AGENT.agentid 
                WHERE AGENT.agentid = %s 
                ORDER BY CONCAT(CUSTOMER.firstname,' ',CUSTOMER.lastname)
            """
        cursor.execute(query,(agentId,))
        userInfo = list(cursor.fetchall())
        booking_list = [{
            'booking_no': str(row[0]),    #booking
            'customer_name': row[1], #customer
            'performance': row[2],   #booking
            'performance_date': row[3], #booking
            'booked_by': row[4],#agent
            'instruction': row[5] #booking
        } for row in userInfo]
    
    conn.close()

    return booking_list


'''
Find a list of bookings based on the searchString provided as parameter
See assignment description for search specification
'''
def findBookingsByCustomerAgentPerformance(searchString):
    # TODO - find a list of bookings in DB based on searchString input

    
    conn = openConnection()
    with conn.cursor() as cursor:
        #enter not empty
        query1 = """ 
                SELECT BOOKING.booking_no,CONCAT(CUSTOMER.firstname,' ',CUSTOMER.lastname),BOOKING.performance,BOOKING.performance_date,CONCAT(AGENT.firstname,' ',AGENT.lastname),BOOKING.instruction
                FROM BOOKING 
                JOIN CUSTOMER ON BOOKING.customer = CUSTOMER.email
                LEFT JOIN AGENT ON BOOKING.booked_by = AGENT.agentid 
                WHERE UPPER(CONCAT(CUSTOMER.firstname,' ',CUSTOMER.lastname)) LIKE %s OR UPPER( CONCAT(AGENT.firstname,' ',AGENT.lastname))LIKE %s OR UPPER(BOOKING.performance) LIKE %s
                ORDER BY CONCAT(CUSTOMER.firstname,' ',CUSTOMER.lastname)
            """
        searchString = searchString.upper()
        searchString = f'%{searchString}%'
        cursor.execute(query1,(searchString,searchString,searchString,))
        booking_db = list(cursor.fetchall())
        booking_list = [{
            'booking_no': str(row[0]),
            'customer_name': row[1],
            'performance': row[2],
            'performance_date': row[3],
            'booked_by': row[4],
            'instruction': row[5]
        } for row in booking_db]
    conn.close()
    return booking_list


#####################################################################################
##  Booking (customer, performance, performance date, booking agent, instruction)  ##
#####################################################################################
'''
Add a new booking into the database - details for a new booking provided as parameters
'''
def addBooking(customer, performance, performance_date, booked_by, instruction):
    # TODO - add a booking
    # Insert a new booking into database
    # return False if adding was unsuccessful
    # return True if adding was successful
    query = ''' 
    INSERT INTO BOOKING (customer,performance,performance_date,booked_by,instruction)
    VALUES(get_customer_email(%s),%s,%s,get_agent_id(%s),%s)
    '''
    state = True
    conn = openConnection()
    with conn.cursor() as cursor:

        try:
            cursor.execute(query,
                           [customer,performance,performance_date,booked_by,instruction])
        except psycopg2.errors.RaiseException:
            state = False
        if not state:
            state =False
        if cursor.rowcount == 0:
            state = False
    if state == True:
        conn.commit()
    else:
        conn.rollback()
    conn.close()
    return state
        


'''
Update an existing booking with the booking details provided in the parameters
'''
def updateBooking(booking_no, performance, performance_date, booked_by, instruction):
    # TODO - update an existing booking in DB
    # return False if updating was unsuccessful
    # return True if updating was successful
    query = """ 
    UPDATE BOOKING 
    SET 
        performance = %s,
        performance_date = %s,
        booked_by = get_agent_id(%s),
        instruction = %s
    where booking_no = %s
    """
    state = True
    conn = openConnection()
    with conn.cursor() as cursor:

        try:
            cursor.execute(query,
                           [performance,performance_date,booked_by,instruction,booking_no])
        except psycopg2.errors.RaiseException:
            state = False
        if cursor.rowcount == 0:
            state =  False
    if(state == True):
        conn.commit()
    if (state == False):
        conn.rollback()
    conn.close()
    return state
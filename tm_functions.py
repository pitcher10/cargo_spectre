import pyodbc as db

def get_dlid(barcode):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"select detail_number from trace where trace_type = 'W' and trace_number = '{barcode}' and detail_number not in (select detail_line_id from tlorder where detail_number = detail_line_id and approved = True)"
    cur.execute(q)
    return cur.fetchall()

def get_sequence(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"select sequence from tldtl where order_id = {dlid} and description <> 'Weight Deficit' "
    cur.execute(q)
    return cur.fetchall()

def update_dims(dlid,length,width,height,sequence):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = "CALL WTS_SPECTRE_ROUTINE(?,?,?,?,?,?)"
    params = (dlid,1,length,width,height,sequence)
    cur.execute(q,params)
    conn.commit()

def delete_previous_dims(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"delete from tldtl_dimensions where dlid = {dlid} and (select user9 from tlorder where detail_line_id = {dlid}) is null "
    cur.execute(q)
    conn.commit()

def update_tlorder(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"update tlorder set user9 = 'SPECTRE' where detail_line_id = {dlid}"
    cur.execute(q)
    conn.commit()

def check_to_be_deleted(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f''' select detail_line_id from tlorder where detail_line_id = {dlid} and 
        approved = False and (
        select count(order_id) from tldtl where order_id = {dlid} 
        and description <> 'Weight Deficit') > 1 '''
    cur.execute(q)
    return cur.fetchall()

def delete_old_dtls(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"delete from tldtl where order_id = {dlid} and sequence > (select min(sequence) from tldtl where order_id = {dlid})"
    cur.execute(q)
    conn.commit()

def update_new_dtls(dlid,pieces,weight):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"update tldtl set pieces = {pieces}, length_1 = 0, width = 0, height = 0, pieces_units = 'PCS', weight = {weight} where order_id = {dlid}"
    cur.execute(q)
    conn.commit()

def check_webservice(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"select detail_line_id from tlorder where detail_line_id = {dlid} and created_by = 'WEBSERVICE'"
    cur.execute(q)
    return cur.fetchall()

def update_admin_status(dlid):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"update tlorder set current_status_admin = 'SPECTRE' where detail_line_id = {dlid}"
    cur.execute(q)
    conn.commit()

def insert_admin_status(dlid,current_timestamp,):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    order_id = dlid
    current_timestamp =  current_timestamp
    status = "SPECTRE"
    comment = "Updated By Spectre"
    trip_number = 0
    leg_number = 0
    zone = ''
    reason_code = ''
    loc_client = ''
    current_status = 'False'
    unmatch = ''
    q = f"CALL INS_FB_STATUS(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    params = (order_id,current_timestamp,status,comment,trip_number,leg_number,zone,reason_code,loc_client,current_status,unmatch)
    cur.execute(q,params)
    conn.commit()

def update_weight(dlid,weight):
    conn = "Driver={IBM DB2 ODBC DRIVER - DB2COPY1};Database=TEST;Hostname=localhost;Port=50000;Protocol=TCPIP;Uid=TMWIN;Pwd=Markussiebert1!;CurrentSchema=TMWIN;"
    conn = db.connect(conn)
    cur = conn.cursor()
    q = f"update tlorder set weight = {weight} where detail_line_id = {dlid}"
    cur.execute(q)
    conn.commit()
    q2 = f"update tldtl set weight = {weight} where order_id = {dlid} and sequence = (select min(sequence) from tldtl where order_id = {dlid})"
    cur.execute(q2)
    conn.commit()



    
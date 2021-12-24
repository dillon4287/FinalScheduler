#!/usr/bin/python3

import sqlite3 as sq
import pandas as pd
# import os
# import argparse
from pyfiglet import Figlet
# import sys
# import curses
pd.options.mode.chained_assignment = None  # default='warn'



def createSlots(numSlots, connection):
    cur = connection.cursor()
    for i in range(numSlots):
        slot = "slot{}".format(i+1)
        print("Creating table {}".format(slot))
        create = "CREATE TABLE IF NOT EXISTS {} (".format(slot)
        col1 = "StudentID INTEGER, "
        col2 = "FirstName String, "
        col3 = "LastName String, "
        col4 = "Type String, "
        col5 = "Course String"
        end = ");"
        sql = create + col1 + col2 + col3 + col4 + col5 + end
        cur.execute(sql)
    print("Successfully created {} slots".format(numSlots))


def printTableInfo(connection):
    print("All tables in db:")
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    l = cursor.fetchall()
    for i in l:
        print(i[0])


def deleteTable(name, connection):
    cursor = connection.cursor()
    cursor.execute("DROP TABLE IF EXISTS {}".format(name))


def importCsv(fname):
    f1 = pd.read_csv(fname, keep_default_na=False)
    f1 = getFields(f1)
    course = ["{}".format(getTableName(fname)) for i in range(f1.shape[0])]
    f1.insert(f1.shape[1], "Course", course)
    return f1


def getFields(dataframe):
    framesubset = dataframe[["Student ID", "First Name", "Last Name", "Type"]]
    framesubset.rename(columns={"Student ID": "StudentID",
                       "First Name": "FirstName", "Last Name": "LastName"}, inplace=True)
    return framesubset


def printExe(execution):
    for i in execution:
        print(i)


def printAll(tablename, connection):
    r = connection.execute("SELECT * FROM {}".format(tablename))
    stuff = []
    for i in r:
        stuff.append(i)
    prettyPrintSql(stuff)


def createTable(fname, connection):
    cur = connection.cursor()
    f1 = importCsv(fname)
    name = getTableName(fname)
    cur.execute("DROP TABLE IF EXISTS {}".format(name))
    create = "CREATE TABLE  {} (".format(name)
    col1 = "StudentID INTEGER, "
    col2 = "FirstName String, "
    col3 = "LastName String, "
    col4 = "Type String, "
    col5 = "Course String"
    end = ");"
    sql = create + col1 + col2 + col3 + col4 + col5 + end
    cur.execute(sql)

    f1.to_sql(name, connection,
              if_exists='append',  index=False)


def getTableName(fname):
    return fname[0:fname.rfind('.')]


def createTempTable(name, connection):
    cur = connection.cursor()
    cur.execute("DROP TABLE IF EXISTS {}".format(name))
    create = "CREATE TABLE {} (".format(name)
    col1 = "StudentID INTEGER, "
    col2 = "FirstName String, "
    col3 = "LastName String, "
    col4 = "Type String, "
    col5 = "Course String"
    end = ");"
    sql = create + col1 + col2 + col3 + col4 + col5 + end
    cur.execute(sql)


def deleteRowsByCourse(tablename, coursename, conn):
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM {} where Course = '{}' ".format(tablename, coursename))


def deleteRowsById(tablename, idnum, conn):
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM {} where StudentID = '{}' ".format(tablename, idnum))


def checkForConlicts(slotNum, candidateList, connection):
    cur = connection.cursor()
    slot = "candidate_slot{}".format(slotNum)
    deleteTable(slot, connection)
    create = "CREATE TABLE {} (\n".format(slot)
    col1 = "StudentID INTEGER, "
    col2 = "FirstName String, "
    col3 = "LastName String, "
    col4 = "Type String, "
    col5 = "Course String"
    end = ");"
    sql = create + col1 + col2 + col3 + col4 + col5 + end
    cur.execute(sql)
    createunion = "INSERT INTO {}\n".format(slot)
    for i in range(len(candidateList)):
        if i == len(candidateList)-1:
            createunion += "SELECT * FROM {}".format(candidateList[i])
        else:
            createunion += "SELECT * FROM {} \nUNION ALL \n".format(
                candidateList[i])
    cur.execute(createunion)
    tmp = "temp"
    createTempTable(tmp, connection)
    duplicates = "INSERT INTO {} \n SELECT * FROM {}\n".format(tmp, slot)
    duplicates += "GROUP BY StudentID\n HAVING COUNT(*) > 1;"
    cur.execute(duplicates)
    cur2 = connection.cursor()
    dupes = []
    for i in cur.execute("SELECT * FROM temp"):
        x = "SELECT * FROM {} WHERE StudentID = {}".format(slot, i[0])
        for j in cur2.execute(x):
            dupes.append(j)
    if (len(dupes) > 0):
        print("\n\tCould not create table because duplicates exist!\n\tRemove a file before proceeding.\n".format(slot))
        print("\tDuplicates")
        prettyPrintSql(dupes)
    else:
        # No conflicts in candidate slot, however, need to check if any exist
        # in existing slot table!
        slot = "slot{}".format(slotNum)
        check = cur.execute("""select {}.StudentID, {}.FirstName, {}.LastName, {}.Type, {}.Course 
        from {} inner join candidate_{} on  {}.StudentID=candidate_{}.StudentID;""".format(slot, slot, slot, slot, slot, slot, slot, slot, slot))
        dupes = []
        for i in check:
            if i[0] == '':
                try:
                    x = "SELECT * FROM {} WHERE LastName = '{}' ".format(
                        slot, i[2])
                    for j in cur2.execute(x):
                        dupes.append(j)
                        dupes.append(i)
                except:
                    print("Record has incomplete incormation, however duplicate exists!")

            else:
                x = "SELECT * FROM {} WHERE StudentID = {}".format(slot, i[0])
                for j in cur2.execute(x):
                    dupes.append(j)
                    dupes.append(i)
        if len(dupes) > 0:
            print(
                "Some student records are found in both {} and the candidate pool.".format(slot))
            print("These students cannot be assigned to this slot:")
            prettyPrintSql(dupes)
        else:
            print("No conflicts found, putting students and faculty into this slot")
            cur.execute(
                "INSERT INTO {} SELECT * FROM candidate_{}".format(slot, slot))
            x = cur.execute("SELECT * FROM {}".format(slot))
            records = []
            for i in x:
                records.append(i)
            print("Students and faculty in {}".format(slot))
            prettyPrintSql(records)


def prettyPrintSql(sql):
    print("------------------------------------------------------------------")
    print("{:<20}{:<20}{:<20}{:<15}".format(
        "LastName", "FirstName", "Type", "Course"))
    print("------------------------------------------------------------------")
    for i in sql:
        print("{:<20}{:<20}{:<20}{:<15}".format(i[1], i[2], i[3], i[4]))

def printCsv(fname):
    f1 = pd.read_csv(fname, keep_default_na=False)
    f1 = getFields(f1)
    print(f1)
    



f = Figlet(font='slant')
print(f.renderText("Final Scheduler"))
print("Usage:")
print("{:<35} {:<20}".format("Create a database for the finals:", "create FinalSchedule"))
print("{:<35} {:<20}".format("Create Numbered Time Slots:", "makeslots 10"))
print("{:<35} {:<20}".format("Assign csv files to the slots (e.g. assign to slot 1):", "assignslot 1 filename1.csv filename2.csv"))
print("{:<35}".format("Check for conflicts and drop files\nif there are or move to the next\nslot."))
print("{:<35} {:<20}".format("Drop any table in the database:", "deletetable NAME"))
print("{:<35} {:<20}".format("Delete a course from a table", "deletebycourse NAME"))
print("{:<35} {:<20}".format("Delete a student from a table:", "deletebystudentid NAME"))
print("{:<35} {:<20}".format("Print all tables in the database:", "printtables"))
print("{:<35} {:<20}".format("Print all records in a table:", "print NAME"))
print("{:<35} {:<20}".format("Execute any sql type statement:", "sql SQL_COMMANDS"))
print("{:<35} {:<20}".format("Print a csv file:", "printcsv FILENAME"))
print("{:<35} {:<20}".format("Save the progress:", "save"))
print("{:<35} {:<20}".format("Quit:", "q or quit"))
print()

while(1):
    c = input("> ")
    clist = c.split(" ")
    if(c.startswith("create ")):
        conn = sq.connect("{}.db".format(clist[1]))
        try:
            cur = conn.cursor()
            print("Connected to db")
        except:
            print("FAILED to connect")
    if clist[0] == "makeslots":
        try:
             nslots = int(clist[1])
             createSlots(nslots, conn)
        except:
            print("Error in input in makeslots")
    if clist[0] == "assignslot":
        names = []
        slotnum = clist[1]
        for i in clist[2:]:
            if i.endswith(".csv"):
                try:
                    createTable(i, conn)
                except:
                    print("No connection or filename incorrect")
                names.append(getTableName(i))
        try:
            checkForConlicts(slotnum, names, conn)
        except:
            print("Possibly no table by that name, create more slots")
    if clist[0] == "save":
        conn.commit()
    if (clist[0] == "q" ) or (clist[0] == "quit" ):
        try:
            conn.commit()
            conn.close()
            exit(1)
        except:
            exit(1)
    if clist[0] == "deletetable":

        print("Deleting table {}".format(clist[1]))
        try: 
            deleteTable(clist[1], conn)
            print("Update table list:")
            printTableInfo(conn)
        except:
            print("No table by that name, none deleted!")
    if clist[0] == "print":
        try:
            printAll(clist[1], conn)
        except:
            print("No table by that name, or no open connection")
            print("Callable tables:")
            try:
                printTableInfo(conn)
            except:
                print("There is no connection!")
                print("Make a database first.")
    if clist[0] == "deletebycourse":
        tablename = clist[1]
        courseid = clist[2]
        deleteRowsByCourse(tablename, courseid, conn)
    if clist[0] == "deletebystudentid":
        tablename = clist[1]
        id = clist[2]
        deleteRowsById(tablename, id, conn)
    if clist[0] == "printtables":
        printTableInfo(conn)
    if clist[0] == "sql":
        stmt = ' '.join(clist[1:])
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        except:
            print("Input error")
    if clist[0] == "printcsv":
        try:
            printCsv(clist[1])
        except:
            print("Input error")




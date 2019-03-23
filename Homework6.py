import mysql.connector

connection = mysql.connector.connect(user='cmpsc431', password='mysql1234', host='127.0.0.1', database='431whw6')

cur = connection.cursor(buffered=True, dictionary=True)

#cleans up the database afterwards
cur.execute("DROP TABLE `works`")
cur.execute("DROP TABLE `emp`, `dept`")
connection.commit()

#create try/except block
cur.execute("CREATE TABLE emp ( eid int(11) NOT NULL, ename text CHARACTER SET utf8 NOT NULL, age int(11) NOT NULL, salary int(11) NOT NULL, PRIMARY KEY (eid)) ENGINE = InnoDB")
cur.execute("CREATE TABLE dept ( did int(11) NOT NULL, budget int(11) NOT NULL, managerid int(11) NOT NULL, PRIMARY KEY (did)) ENGINE = InnoDB")
cur.execute("CREATE TABLE works ( eid int(11) NOT NULL, did int(11) NOT NULL, pct_time int(11) NOT NULL, PRIMARY KEY (eid,did)) ENGINE=InnoDB")

line = open("hw6-dataset-sp19.txt", "r")

# parses text file and populates MYSQL tables via INSERT

entry = "g"
shift = 0
while len(entry) > 0 :
    entry = line.readline()
    submit = entry.split(',')
    shift+=1
    if shift >= 3 and shift <= 62:
        eeid = int(submit[0].strip())
        eename = submit[1].strip()
        eage = int(submit[2].strip())
        esalary = submit[3].strip()
        esalary = int(esalary[0:-2])
        cur.execute(("INSERT INTO emp (eid, ename, age, salary) VALUES (%d, %s, %d, %d);" % (eeid, eename, eage, esalary)))
    elif shift >= 66 and shift <=165:
        weid = int(submit[0].strip())
        wdid = int(submit[1].strip())
        wpcttime = int(submit[2].strip())
        cur.execute("INSERT INTO works (eid, did, pct_time) VALUES (%d, %d, %d);" % (weid, wdid, wpcttime))
    elif shift >= 169 and shift <= 179:
        ddid = int(submit[0].strip())
        dbudget = submit[1].strip()
        dbudget = int(dbudget[0:-2])
        dmanagerid = int(submit[2].strip())
        cur.execute("INSERT INTO dept (did, budget, managerid) VALUES (%d, %d, %d);" % (ddid, dbudget, dmanagerid))

line.close()
connection.commit()

#add constraints after populating the tables
cur.execute("ALTER TABLE `works` ADD CONSTRAINT `total participation` FOREIGN KEY (`did`) REFERENCES `dept`(`did`) ON DELETE NO ACTION ON UPDATE NO ACTION")
cur.execute("ALTER TABLE `works` ADD CONSTRAINT `total participation 2` FOREIGN KEY (`eid`) REFERENCES `emp`(`eid`) ON DELETE NO ACTION ON UPDATE NO ACTION")

connection.commit()

# Question 1
# search through each did to find an eid that matches the age
#cur.execute("SELECT did, MAX(age), emp.eid, ename, salary FROM `emp`, `works` WHERE emp.eid=works.eid GROUP BY did")
cur.execute("SELECT w2.did, e2.eid, e2.ename, e2.age, e2.salary FROM Emp e2 JOIN Works w2 USING (eid) JOIN (SELECT w.did, MAX(e.age) AS Oldest FROM works w INNER JOIN emp e USING (eid) GROUP BY w.did) sq ON w2.did = sq.did and e2.age = sq.Oldest ORDER BY `w2`.`did` ASC")
for (did) in cur:
    print(did)

#do stuff here
connection.commit()



#cleans up the database afterwards
#cur.execute("DROP TABLE `works`")
#cur.execute("DROP TABLE `emp`, `dept`")
#connection.commit()

cur.close()
connection.close()
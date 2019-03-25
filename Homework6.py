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



# parses text file and populates MYSQL tables via INSERT
line = open("hw6-dataset-sp19.txt", "r")
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
# nested join to collect max age from each did, then join to find the eid with that age in that department
cur.execute("SELECT w2.did, e2.eid, e2.ename, e2.age, e2.salary FROM emp e2 LEFT OUTER JOIN Works w2 USING (eid) JOIN (SELECT w.did, MAX(e.age) AS Oldest FROM works w INNER JOIN emp e USING (eid) GROUP BY w.did) old ON w2.did = old.did and e2.age = old.Oldest ORDER BY `w2`.`did` ASC")
print("Question #1")
for (did) in cur:
    print(did)

print("")

# Question 2
# Find the employees who are younger than all managers (assuming managerid=emp.eid)
cur.execute("SELECT e2.eid, e2.ename, e2.age FROM emp e2 WHERE e2.age < (SELECT MIN(e.age) AS Youngest FROM emp e RIGHT OUTER JOIN dept d ON e.eid=d.managerid) GROUP BY e2.eid")
print("Question #2")
for (managerid) in cur:
    print(managerid)

print("")

# Question 3
# Find the names, age and salary of each employee who works in the Sales Department but not in Marketing department (assume that sales did=4, and marketing did=6)
cur.execute("SELECT e.eid, e.ename, e.age, e.salary FROM emp e JOIN (SELECT w.eid FROM works w WHERE w.did=4 AND w.eid NOT IN (SELECT w.eid FROM works w WHERE w.did=6) GROUP BY w.eid) dubya ON e.eid=dubya.eid")
print("Question #3")
for (eid) in cur:
    print(eid)

print("")

# Question 4
# Find the department with the highest average salary of employees
cur.execute("SELECT w.did, AVG(t.salary) FROM works w JOIN (SELECT e.eid, e.salary FROM emp e) t ON t.eid=w.eid GROUP BY w.did")
print("Question #4")
for (eid) in cur:
    print(eid)

print("")

# Question 5
# Find the managers who also work in another department(s) but not as a manager there
cur.execute("SELECT t2.managerid, t2.did FROM (SELECT DISTINCT md.managerid, w.did FROM works w JOIN (SELECT d.managerid FROM dept d) md ON w.eid=md.managerid) AS t2 LEFT OUTER JOIN (SELECT d2.managerid, d2.did FROM dept d2) AS t3 ON t2.managerid=t3.managerid AND t2.did=t3.did WHERE t3.did IS NULL")
print("Question #5")
for (managerid) in cur:
    print(managerid)

print("")

# Question 6
# Find the employee ID, name and age of each employee whose salary exceeds the budget of every department that he or she works in
# Returns null because no employee has a salary higher than ALL departments that they work in. Only eid=21 has a salary higher than ONE of the departments that they work in.
cur.execute("SELECT e.eid, e.ename, e.age FROM emp e JOIN(SELECT w.eid, w.did, MAX(bd.budget) AS yas FROM works w JOIN (SELECT d.did, d.budget FROM dept d) bd ON w.did=bd.did GROUP BY w.eid) ma ON e.eid=ma.eid WHERE e.salary > yas")
print("Question #6")
for (eid) in cur:
    print(eid)

print("")

connection.commit()


cur.close()
connection.close()

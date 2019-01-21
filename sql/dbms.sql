-- 1
create database testDB;

-- 2
create table testTBL (S# char(8) not null, Sname char(10), Ssex char(2));

-- 3
insert into Student values('98030101', 'zhangsan', 'nan');
insert into Student (S#, Sname, Ssex) values ('98030101', 'zhangsan', 'nan');

-- 4
select S# from sc where c# = '002' and Score > 80 order by Score DESC;

-- 5
delete from sc where S# = '98030101';

-- 6
delete from Student where D# in (Select D# from Dept where Dname = 'zidongkongzhi');

-- 7
update teacher set salary = salary * 1.05;

-- 8
update teacher set salary = salary * 1.1 where D# in (select D# from Dept where Dname = 'jisuanji');

-- 9
alter table Student add saddr char[40], PID char[18];

-- 10
alter table Student modify sname char(10);

-- 11
alter table Student drop unique(Sname);

-- 12
drop table Student;

-- 13
drop database SCT;

-- 14
use testDB;

-- 15
close testDB;

-- 16
grant select on Student to rjy;
grant update on Student to rjy;

-- dbms 07

-- 列出选修了001号课程的学生的学号和姓名
select S#, Sname from Student
where S# in (Select S# from SC where C#='001');

-- 求即学过001号课程，又学过002
select S# from SC
where C# = '001' and S# in (select S# from SC where C# = '002');

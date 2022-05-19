## 可能出现问题的测试序列

### 1

Client A:

```
>>> create table person(id int);
```

Client B:

```
>>> drop table person;
```

Client A:

```
>>> select * from person;
# 或者
>>> insert into person values(1323);
```

### 2

Client A:

```
>>> create table person(id int);
```

Client B:

```
>>> drop table person;
>>> create table person(id int);
```

Client A:

```
>>> select * from person;
或者
>>> insert into person values(132);
```

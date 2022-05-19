from client.errors import ParseError
from client.interpreter import Interpreter


if __name__ == '__main__':
    interpreter = Interpreter()
    lines = [
        # Malformed
        'create table (wrong_name) (col int);',

        'create table person (',
        '  int id,',

        "drop table person;d",
        "create table person();",
        "insert into person values();",

        "select id, name, height from person where id <= 235 and name = 'Mark Twain\\'s sister and height <> 332;",

        # Correct
        '',
        'create table person(id int);',

        'create table person (',
        '  id int,',
        '  name char(50) unique,',
        '  height float,',
        '  primary key(id)',
        ');',

        'create index name_idx on person(name);',
        'create index name_idx on person (name, height);',

        'drop table person;',

        "insert into person values(592, 325.4, 'Mark Twain\\'s Sister\\'s Brother\\'s Nephew', '');",

        "select * from person;",
        "select id, name, height from person;",
        "select * from person where id=235;",
        "select id, name, height from person where id <= 235 and name = 'Mark Twain\\'s sister' and height <> 332.5;",

        "delete from person;",
        "delete from person where name<>'me';",
        "delete from person where id <= 235 and name = 'Mark Twain\\'s sister' and height <> 332.5;",
    ]
    status = ''
    for line in lines:
        try:
            if interpreter.parse(line):
                status = ''
            else:
                status = 'Not finished'
        except ParseError as e:
            print(e)
            status = ''
    if status:
        print(status)

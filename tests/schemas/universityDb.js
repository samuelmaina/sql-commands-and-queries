exports.dept = {
    name: "department",
    schema: `
        (dept name varchar(20),
        building varchar(15),
        budget numeric(12, 2),
        primary key(dept name));
  `
}


exports.course = {
    name: "course",
    schema: `
         (course id varchar(7),
        title varchar(50),
        dept name varchar(20),
        credits numeric(2, 0),
        primary key(course id),
        foreign key(dept name) references department);
    `
}

exports.instructor = {
    name: "instructor",
    schema: `
         (ID varchar(5),
        name varchar(20) not null,
        dept name varchar(20),
        salary numeric(8, 2),
        primary key(ID),
        foreign key(dept name) references department); `
}




exports.section = {
    name: "section",
    schema: `(course id varchar(8),
                sec id varchar(8),
                semester varchar(6),
                year numeric(4, 0),
                building varchar(15),
                room number varchar(7),
                time slot id varchar(4),
                primary key(course id, sec id, semester, year),
                foreign key(course id) references course); `
}


exports.teaches = {
    name: "teaches",
    schema: `(ID varchar(5),
            course id varchar(8),
            sec id varchar(8),
            semester varchar(6),
            year numeric(4, 0),
            primary key(ID, course id, sec id, semester, year),
            foreign key(course id, sec id, semester, year) references section,
            foreign key(ID) references instructor);  `
}


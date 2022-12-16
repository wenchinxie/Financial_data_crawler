\connect postgres

CREATE TABLE Raw_material (
    id serial not null,
    Date TIMESTAMP,
    Material_name VARCHAR(255) NOT NULL,
    Price DOUBLE PRECISION NOT NULL,
    primary key(Date,Material_name)
);

CREATE TABLE News (
    id serial not null,
    Date TIMESTAMP,
    Headline VARCHAR(255) NOT NULL,
    Tags VARCHAR(255) NOT NULL,
    Content VARCHAR(255) NOT NULL,
    primary key(Date,Headline)
);
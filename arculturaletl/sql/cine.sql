CREATE TABLE cine (
    id                  SERIAL NOT NULL PRIMARY KEY, 
    provincia           VARCHAR(80),
    cant_pantallas      INT,
    cant_butacas        INT,
    cant_espacios_incaa INT,
    fecha_carga         TIMESTAMP WITH TIME ZONE NOT NULL
);

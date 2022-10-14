CREATE TABLE lugar (
    id                  SERIAL NOT NULL PRIMARY KEY,
    cod_localidad       INT,         
    id_provincia        INT,           
    id_departamento     INT,        
    categoria           VARCHAR(20),
    provincia           VARCHAR(80),
    localidad           VARCHAR(80),
    nombre              VARCHAR(80),
    domicilio           VARCHAR(100),
    codigo_postal       VARCHAR(15),
    mail                VARCHAR(50),
    web                 VARCHAR(100),
    telefono            VARCHAR(20),
    fecha_carga         TIMESTAMP WITH TIME ZONE NOT NULL
);
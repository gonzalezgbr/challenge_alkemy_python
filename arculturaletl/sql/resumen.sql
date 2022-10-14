CREATE TABLE resumen (
    id          SERIAL NOT NULL PRIMARY KEY,
    clase       VARCHAR(50),
    valor       VARCHAR(80),
    total       INT,
    fecha_carga TIMESTAMP WITH TIME ZONE NOT NULL
);
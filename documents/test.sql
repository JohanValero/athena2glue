WITH vw_data_a as (
    SELECT  id, nombre_institucion, SUM(estudiantes) as total_estudiantes
    FROM    catalogo.database.TABLE_INSTITUCION
    WHERE   fecha_cargo > 20251208
), vw_data_b as (
    SELECT  id, nombre, total_estudiantes, COUNT(DISTINCT auditorias) as total_auditorias
    FROM    vw_data_a vwa
            LEFT JOIN
            logs.TABLE_AUDITORIA ta
        ON  ta.ID_ESTUDIANTE = vwa.id
)
SELECT  id, nombre_institucion, total_estudiantes, total_auditorias
FROM    vw_data_b
        INNER JOIN
        logs.TABLE_AUDITORIA ta
    ON  b.id = ta.ID_ESTUDIANTE
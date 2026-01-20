WITH ultimos_4_dias AS (
    SELECT fec_fecha_iso,
           flg_dia_habil
    FROM dwh_thr_modelo_datos.dim_tiempo
    WHERE id_dh_fecha < 20251107
    ORDER BY fec_fecha_iso DESC
    LIMIT 4
),
rango AS (
    SELECT MIN(fec_fecha_iso) AS fecha_inicio,
           MAX(fec_fecha_iso) AS fecha_final
    FROM ultimos_4_dias
),
habiles AS (
    SELECT t.fec_fecha_iso,
           row_number() OVER (ORDER BY t.fec_fecha_iso ASC) AS columna
    FROM ultimos_4_dias t
    WHERE t.flg_dia_habil = 1
    ORDER BY t.fec_fecha_iso
),
no_habiles AS (
    SELECT t.fec_fecha_iso,
           CASE
               WHEN t.nom_dia_semana = 'sábado' THEN 0
               ELSE t.flg_dia_habil
           END AS flg_dia_habil,
           row_number() OVER (ORDER BY t.fec_fecha_iso ASC) AS columna
    FROM dwh_thr_modelo_datos.dim_tiempo t
    JOIN rango r
      ON t.fec_fecha_iso >= r.fecha_inicio
     AND t.id_dh_fecha < 20251107
),
fin_de_semana AS (
    SELECT t.fec_fecha_iso,
           t.nom_dia_semana,
           t.flg_dia_habil
    FROM dwh_thr_modelo_datos.dim_tiempo t
    INNER JOIN rango r
            ON t.fec_fecha_iso < r.fecha_inicio
    ORDER BY fec_fecha_iso DESC
    LIMIT 4
),
analisis AS (
    SELECT t.*,
           lead(t.flg_dia_habil) OVER (ORDER BY t.fec_fecha_iso ASC) AS flg_siguiente,
           lead(t.nom_dia_semana) OVER (ORDER BY t.fec_fecha_iso ASC) AS dia_siguiente
    FROM fin_de_semana t
),
rd_fechas AS (
    SELECT nom_dia_semana AS proyeccion, fec_fecha_iso AS fecha, 1 AS columna
    FROM analisis
    WHERE (flg_dia_habil = 0 OR nom_dia_semana = 'sábado')
        AND NOT (nom_dia_semana = 'jueves' AND flg_dia_habil = 0 
                 AND dia_siguiente = 'viernes' AND flg_siguiente = 1)
    UNION ALL
    SELECT nom_dia_semana AS proyeccion, fec_fecha_iso AS fecha,
        CASE
            WHEN flg_dia_habil = 1 THEN columna_h
            WHEN flg_dia_habil = 0 AND nom_dia_semana = 'jueves' THEN 5
            ELSE columna_nh - columna_ref + 1
        END AS columna
    FROM (
        SELECT t.fec_fecha_iso, nh.flg_dia_habil, t.nom_dia_semana,
               h.columna AS columna_h, nh.columna AS columna_nh,
               row_number() OVER (PARTITION BY nh.flg_dia_habil ORDER BY t.fec_fecha_iso ASC) AS columna_ref
        FROM dwh_thr_modelo_datos.dim_tiempo t
        INNER JOIN no_habiles nh ON t.fec_fecha_iso = nh.fec_fecha_iso
        LEFT JOIN habiles h ON t.fec_fecha_iso = h.fec_fecha_iso
    )
    UNION ALL
    SELECT nom_dia_semana AS proyeccion, fec_fecha_iso AS fecha,
        CASE WHEN flg_dia_habil = 1 THEN 5 ELSE 8 END AS columna
    FROM dwh_thr_modelo_datos.dim_tiempo
    WHERE id_dh_fecha = 20251107
    UNION ALL
    SELECT CASE WHEN flg_dia_habil = 1 AND nom_dia_semana <> 'sábado' 
                THEN '1' ELSE nom_dia_semana END AS proyeccion,
           fec_fecha_iso AS fecha, 8 AS columna
    FROM dwh_thr_modelo_datos.dim_tiempo
    WHERE fec_fecha_iso <= (
            SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
            WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
              AND nom_dia_semana <> 'sábado'
            ORDER BY fec_fecha_iso ASC LIMIT 1)
      AND id_dh_fecha > 20251107
    UNION ALL
    (SELECT cast(row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 1 AS varchar) AS proyeccion,
            t.fec_fecha_iso AS fecha,
            row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 8 AS columna
     FROM dwh_thr_modelo_datos.dim_tiempo t
     WHERE t.fec_fecha_iso > (
            SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
            WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
              AND nom_dia_semana <> 'sábado'
            ORDER BY fec_fecha_iso ASC LIMIT 1)
     ORDER BY t.fec_fecha_iso ASC LIMIT 6)
    UNION ALL
    (SELECT cast(row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 7 AS varchar) AS proyeccion,
            t.fec_fecha_iso AS fecha, 16 AS columna
     FROM dwh_thr_modelo_datos.dim_tiempo t
     WHERE t.fec_fecha_iso > date_add('day', 6,
            (SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
             WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
               AND nom_dia_semana <> 'sábado'
             ORDER BY fec_fecha_iso ASC LIMIT 1))
     ORDER BY t.fec_fecha_iso ASC LIMIT 8)
    UNION ALL
    (SELECT cast(row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 15 AS varchar) AS proyeccion,
            t.fec_fecha_iso AS fecha, 17 AS columna
     FROM dwh_thr_modelo_datos.dim_tiempo t
     WHERE t.fec_fecha_iso > date_add('day', 14,
            (SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
             WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
               AND nom_dia_semana <> 'sábado'
             ORDER BY fec_fecha_iso ASC LIMIT 1))
     ORDER BY t.fec_fecha_iso ASC LIMIT 15)
    UNION ALL
    (SELECT cast(row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 30 AS varchar) AS proyeccion,
            t.fec_fecha_iso AS fecha, 19 AS columna
     FROM dwh_thr_modelo_datos.dim_tiempo t
     WHERE t.fec_fecha_iso > date_add('day', 29,
            (SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
             WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
               AND nom_dia_semana <> 'sábado'
             ORDER BY fec_fecha_iso ASC LIMIT 1))
     ORDER BY t.fec_fecha_iso ASC LIMIT 60)
    UNION ALL
    (SELECT cast(row_number() OVER (ORDER BY t.fec_fecha_iso ASC) + 90 AS varchar) AS proyeccion,
            t.fec_fecha_iso AS fecha, 20 AS columna
     FROM dwh_thr_modelo_datos.dim_tiempo t
     WHERE t.fec_fecha_iso > date_add('day', 89,
            (SELECT fec_fecha_iso FROM dwh_thr_modelo_datos.dim_tiempo
             WHERE id_dh_fecha > 20251107 AND flg_dia_habil = 1 
               AND nom_dia_semana <> 'sábado'
             ORDER BY fec_fecha_iso ASC LIMIT 1))
     ORDER BY t.fec_fecha_iso ASC LIMIT 275)
),


saldos_detallado AS (
    SELECT
        CAST(uc AS VARCHAR) AS uc,
        CAST(sub AS VARCHAR) AS sub,
        CAST(cuenta AS VARCHAR) AS cuenta,
        CAST(fecha AS DATE) AS fecha,
        CAST(saldo AS DECIMAL) AS saldo
    FROM dwh_thr_reportes.fct_saldos_semanal_detallado
    WHERE CAST(fecha AS DATE) = DATE '2025-11-07'
      AND tipo_corte = 'S'
),

uc_08_1 AS (
    SELECT uc, sub, cuenta, fecha, saldo
    FROM saldos_detallado
    WHERE uc = '08'
),

uc_08_2 AS (
    SELECT 
        UC,
        FECHA,
        SUM(SALDO) AS Valor
    FROM uc_08_1
    GROUP BY UC, FECHA
),

uc_08_2117 AS (
    SELECT 
        UC,
        FECHA,
        SUM(SALDO) AS Valor
    FROM uc_08_1
    WHERE CUENTA = '2117'
    GROUP BY UC, FECHA
),

uc_08_2117_2 AS (
    SELECT 
        t1.UC,
        t1.FECHA,
        t3.Columna,
        t1.Valor AS Valor_Contable,
        t2.Valor AS Valor_2117
    FROM uc_08_2 t1
    INNER JOIN uc_08_2117 t2 
        ON t1.UC = t2.UC AND t1.FECHA = t2.FECHA
    INNER JOIN rd_fechas t3 
        ON t1.FECHA = t3.Fecha
),


uc_08_035 AS (
    SELECT 
        '08' AS UC,
        '035' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.minoristas, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_040 AS (
    SELECT 
        '08' AS UC,
        '040' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.pymes_y_pnm, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_045 AS (
    SELECT 
        '08' AS UC,
        '045' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.entidades_financieras_vigiladas, 0) - COALESCE(t1.fics_abiertos_spp, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_050 AS (
    SELECT 
        '08' AS UC,
        '050' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.fics_abiertos_spp, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_055 AS (
    SELECT 
        '08' AS UC,
        '055' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.sector_gobierno_no_financiero, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_060 AS (
    SELECT 
        '08' AS UC,
        '060' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.mayoristas_extranjeros, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_065 AS (
    SELECT 
        '08' AS UC,
        '065' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.mayoristas_sector_real_y_png, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_070 AS (
    SELECT 
        '08' AS UC,
        '070' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.depositos_judiciales, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_trad t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),


uc_08_035_p AS (
    SELECT 
        '08' AS UC,
        '035' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.minoristas, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_040_p AS (
    SELECT 
        '08' AS UC,
        '040' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.pymes_y_pnm, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_045_p AS (
    SELECT 
        '08' AS UC,
        '045' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.entidades_financieras_vigiladas, 0) - COALESCE(t1.fics_abiertos_spp, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_050_p AS (
    SELECT 
        '08' AS UC,
        '050' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.fics_abiertos_spp, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_055_p AS (
    SELECT 
        '08' AS UC,
        '055' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.sector_gobierno_no_financiero, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_060_p AS (
    SELECT 
        '08' AS UC,
        '060' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.mayoristas_extranjeros, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_065_p AS (
    SELECT 
        '08' AS UC,
        '065' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.mayoristas_sector_real_y_png, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),

uc_08_070_p AS (
    SELECT 
        '08' AS UC,
        '070' AS SUB,
        t2.Columna,
        SUM(COALESCE(t1.depositos_judiciales, 0)) AS Valor
    FROM stg_cap.stg_segmentacion_saldos_pib t1
    INNER JOIN rd_fechas t2 ON CAST(t1.fecha AS DATE) = t2.Fecha
    WHERE t2.Columna <= 5
        AND t2.proyeccion NOT IN ('Sábado', 'Sabado', 'Domingo', 'sabado', 'sábado', 'domingo')
    GROUP BY t2.Columna
),


-- Total TRAD
uc_08_3 AS (
    SELECT UC, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_035
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_040
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_045
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_050
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_055
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_060
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_065
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_070
    ) t
    GROUP BY UC, Columna
),

-- Total PIB
uc_08_3_p AS (
    SELECT UC, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_035_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_040_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_045_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_050_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_055_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_060_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_065_p
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_070_p
    ) t
    GROUP BY UC, Columna
),

-- Unión de TRAD + PIB (equivalente a OUTER UNION CORR)
uc_08_3_pre AS (
    SELECT UC, Columna, Valor FROM uc_08_3
    UNION ALL
    SELECT UC, Columna, Valor FROM uc_08_3_p
),

-- Total final (suma TRAD + PIB)
uc_08_3_total AS (
    SELECT UC, Columna, SUM(Valor) AS Valor
    FROM uc_08_3_pre
    GROUP BY UC, Columna
),

uc_08_4 AS (
    SELECT 
        t2.UC,
        t2.FECHA,
        t2.Columna,
        ABS(t2.Valor_Contable) AS Valor_Contable,
        ABS(t2.Valor_2117) AS Valor_2117,
        t1.Valor AS Valor_Formato,
        (t1.Valor + ABS(t2.Valor_2117)) - ABS(t2.Valor_Contable) AS Valor_Diferencia
    FROM uc_08_3_total t1
    INNER JOIN uc_08_2117_2 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
    WHERE t2.Valor_Contable <> 0
),


uc_08_035_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_035
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_035_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_040_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_040
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_040_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_045_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_045
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_045_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_050_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_050
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_050_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_055_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_055
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_055_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_060_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_060
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_060_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_065_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_065
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_065_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_070_total AS (
    SELECT UC, SUB, Columna, SUM(Valor) AS Valor
    FROM (
        SELECT UC, SUB, Columna, Valor FROM uc_08_070
        UNION ALL
        SELECT UC, SUB, Columna, Valor FROM uc_08_070_p
    ) t
    GROUP BY UC, SUB, Columna
),

uc_08_035_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_035_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_035_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_035_2
),

uc_08_040_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_040_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_040_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_040_2
),

uc_08_045_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_045_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_045_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_045_2
),

uc_08_050_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_050_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_050_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_050_2
),

uc_08_055_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_055_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_055_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_055_2
),

uc_08_060_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_060_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_060_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_060_2
),

uc_08_065_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC,
        (t2.Valor_Contable - t2.Valor_Formato) + t1.Valor AS Valor_Ajustado
    FROM uc_08_065_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_065_def AS (
    SELECT UC, SUB, Columna, Valor AS Valor
    FROM uc_08_065_2
),

uc_08_070_2 AS (
    SELECT 
        t1.UC,
        t1.SUB,
        t1.Columna,
        t1.Valor,
        (t1.Valor * 100) / t2.Valor_Formato AS Porcentaje_Prorateo,
        (t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100 AS Valor_Prorateo,
        t1.Valor + ((t2.Valor_Diferencia * ((t1.Valor * 100) / t2.Valor_Formato)) / 100) AS Valor_SFC
    FROM uc_08_070_total t1
    INNER JOIN uc_08_4 t2 
        ON t1.UC = t2.UC 
        AND t1.Columna = t2.Columna
),

uc_08_070_def AS (
    SELECT UC, SUB, Columna, Valor_SFC AS Valor
    FROM uc_08_070_2
)

SELECT * FROM uc_08_035_def
UNION ALL
SELECT * FROM uc_08_040_def
UNION ALL
SELECT * FROM uc_08_045_def
UNION ALL
SELECT * FROM uc_08_050_def
UNION ALL
SELECT * FROM uc_08_055_def
UNION ALL
SELECT * FROM uc_08_060_def
UNION ALL
SELECT * FROM uc_08_065_def
UNION ALL
SELECT * FROM uc_08_070_def
ORDER BY SUB, Columna
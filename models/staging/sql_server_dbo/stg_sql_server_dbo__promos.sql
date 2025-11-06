WITH source_data AS (

    SELECT
        -- 1. Promociones Reales (desde la fuente)
        MD5(COALESCE(CAST(PROMO_ID AS VARCHAR), '')) AS promo_sk,
        
        CAST(PROMO_ID AS VARCHAR) AS promo_id,
        
        CAST(DISCOUNT AS float) AS discount_value_in_dollars, 
        
        -- Campo Booleano (1/0)
        CASE 
            WHEN STATUS = 'active' THEN 1 
            ELSE 0 
        END AS esta_activo,
        
        STATUS AS promo_status, 

        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at,
        
        _FIVETRAN_DELETED AS is_deleted

    FROM 
        {{ source('SQL_SERVER_DBO', 'PROMOS') }}

    WHERE PROMO_ID IS NOT NULL

    UNION ALL

    -- 2. Fila extra para "Sin Promoción"
    SELECT
        -- Clave constante
        MD5('SIN_PROMOCION') AS promo_sk, 
        
        -- ID de negocio
        CAST('NO_PROMO' AS VARCHAR) AS promo_id,
        
        -- Valor de descuento cero (FLOAT)
        CAST(0.00 AS float) AS discount_value_in_dollars, 
        
        -- Valor constante para is_active (0)
        0 AS esta_activo,
        
        -- Estado por defecto (VARCHAR)
        CAST('active' AS VARCHAR) AS promo_status, 

        -- Marca de tiempo (TIMESTAMP)
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP) AS loaded_at,
        
        -- Bandera de eliminación (NULL)
        CAST(NULL AS BOOLEAN) AS is_deleted 

)

SELECT * FROM source_data
WITH source_data AS (

    SELECT
        -- CLAVE FORÁNEA

        MD5(COALESCE(CAST(PROMO_ID AS VARCHAR), '')) AS promo_sk,
        
        -- CLAVES NATURALES
        CAST(PROMO_ID AS VARCHAR) AS promo_id,
        
        -- ATRIBUTOS

        CAST(DISCOUNT AS float) AS discount_value_in_dollars, 
        
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

    -- AÑADIR Fila extra para "Sin Promoción"
    SELECT
        -- Clave constante
        MD5('SIN_PROMOCION') AS promo_sk, 
        
        CAST('NO_PROMO' AS VARCHAR) AS promo_id,
        
        CAST(0.00 AS float) AS discount_value_in_dollars, 
        
        0 AS esta_activo,
        
        CAST('active' AS VARCHAR) AS promo_status, 

        -- TIEMPO

        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP) AS loaded_at,
        
        CAST(NULL AS BOOLEAN) AS is_deleted 

)

SELECT * FROM source_data
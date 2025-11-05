WITH source_data AS (

    SELECT
        -- 1. CLAVE SUBROGADA 
        {{ dbt_utils.surrogate_key(['PROMO_ID']) }} AS promo_sk, -- Genera un hash único a partir de PROMO_ID
        
        -- 2. Clave Natural (Original)
        PROMO_ID AS promo_id, -- Mantenemos el ID original para referencias
        
        -- 3. Formato del Descuento
        CAST(DISCOUNT AS VARCHAR) || '$' AS discount_value, -- Convierte a texto y añade el símbolo '$'
        
        -- 4. Otros Datos
        STATUS AS promo_status,

        -- 5. Fechas a UTC (si el campo no tiene la zona horaria ya)
        -- Si _FIVETRAN_SYNCED es un TIMESTAMP_TZ (con zona horaria), usamos CONVERT_TIMEZONE.
        -- Si es TIMESTAMP sin zona, usamos TO_TIMESTAMP_NTZ. 
        -- Asumo que es un TIMESTAMP_TZ por el nombre, por lo que lo convertimos a UTC.
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at_utc,
        
        -- Metadatos
        _FIVETRAN_DELETED AS is_deleted

    FROM 
        {{ source('SQL_SERVER_DBO', 'PROMOS') }}
)

SELECT * FROM source_data
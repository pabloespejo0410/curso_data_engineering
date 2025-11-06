WITH source_data AS (

    SELECT

        CAST(USER_ID AS VARCHAR) AS user_id,
        
        -- Clave Foránea (FK) a la Dimensión de Direcciones
        CAST(ADDRESS_ID AS VARCHAR) AS address_fk,
        
        -- Atributos de Identificación y Contacto
        CAST(FIRST_NAME AS VARCHAR) AS first_name,
        CAST(LAST_NAME AS VARCHAR) AS last_name,
        -- Concatenación de nombre completo
        TRIM(first_name || ' ' || last_name) AS full_name,
        CAST(EMAIL AS VARCHAR) AS email,
        CAST(PHONE_NUMBER AS VARCHAR) AS phone_number,
        
        -- Métricas de Usuario
        CAST(TOTAL_ORDERS AS NUMERIC(38, 0)) AS total_orders,
        
        -- Metadatos de Fecha/Hora
        CONVERT_TIMEZONE('UTC', CREATED_AT) AS created_at,
        CONVERT_TIMEZONE('UTC', UPDATED_AT) AS updated_at,
        
        -- Metadatos de Fivetran (Sólo loaded_at)
        CONVERT_TIMEZONE('UTC', _FIVETRAN_SYNCED) AS loaded_at

    FROM 
        {{ source('SQL_SERVER_DBO', 'USERS') }} 

    WHERE USER_ID IS NOT NULL 

)

SELECT * FROM source_data
SELECT
    address_key,
    country,
    zipcode
FROM 
    {{ ref('stg_addresses') }} 
WHERE
    -- Al buscar los distintos paises dentro de country solo aparece United States, 
    -- el código postal de eeuu debe tener exactamente 5 dígitos, por lo que usamos esto
    -- si hubiera mas paises podriamos añadir un OR
    (COUNTRY = 'United States' AND LENGTH(CAST(ZIPCODE AS VARCHAR)) != 5)
    

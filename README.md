1. Para los ids nulos ¿Qué sugieres hacer con ellos ?

Depnde de las necesidades del banco, debido a que tenemos una columna de status que nos dice por qué no ha sido pagado, no veo por que modificarlos.

2. Considerando las columnas name y company_id ¿Qué inconsistencias notas
y como las mitigas?

Parecieran ser redundantes ya que cada 'name' único tiene su 'company_id' único, aunque se observa que en ocasiones, tanto 'name' o 'company_id', no fueron ingresados correctamente o no fueron ingresados, por lo que las redundancias ayudan a poder corregir esos errores.

3. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo
procedes?

Hay 'id' sin valores, habría que buscar generarlo, como parecen ser encriptaciones, deberíamos correr la lógica de la encriptación y generar el 'id', de no poder, poner en su lugar un ID temporal hasta decidir con el equipo qué hacer con él y documentar cada caso. 


4. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones?

Tal vez separar cada parte del proceso, así cuando ocurra un error sepamos exactamente en qué parte del proceso sucedió.

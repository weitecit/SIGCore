# Contrato de datos MongoDB del backend Weitec

> Referencia de esquema de las colecciones MongoDB que usa el backend Weitec, escrita para
> consumidores **Python** (`pymongo`) que necesitan leer y escribir los mismos datos.

> **Fuente de verdad.** Este documento está verificado contra el modelo Go del backend, en
> `C:\Repos\Weitec_back\back\app\models\` (`block.go`, `block_weather.go`, `block_operation.go`,
> `block_operation_task.go`, `block_report.go`, `block_alert.go`, `block_space.go`, `block_layer.go`,
> `block_config.go`, `alert_point.go`, `layer.go`, `kpi.go`, `parcel.go`, `field.go`). Los modelos
> Go son structs con etiquetas `bson:`/`json:`; el nombre real del campo en MongoDB es el de la
> etiqueta **`json:`** cuando el documento pasa por `SetProperties` (que serializa a JSON y lo
> deserialize a un mapa libre) y el de la etiqueta `bson:` cuando el campo lo escribe el encoder
> BSON directamente. Esa diferencia explica casi todas las rarezas de tipo de este esquema.
>
> Los tipos **numéricos** y los **formatos mixtos** se han contrastado además contra los datos
> reales de la base `main` (ver §2.4, §5.2 y §5.4), porque el modelo Go por sí solo no revela el
> viaje por JSON que convierte los enteros en `float64`.

---

## 1. Alcance

Este documento describe **la forma de los datos**, no el código que los produce. Está pensado para
que un servicio Python pueda:

- leer las mismas colecciones que el backend principal,
- escribir documentos que el backend principal siga entendiendo,
- saber qué campos existen, con qué tipo, y qué significa cada uno.

Todo el esquema descrito aquí corresponde a la **base de datos `main`** de una instancia MongoDB.
La aplicación que es propietaria de estos datos es un servicio escrito en Go; desde el punto de
vista de Python es simplemente "la otra aplicación que escribe en la misma base".

Tres familias de colecciones:

| Familia | Quién escribe | Interés para Python |
|---|---|---|
| **Núcleo geoespacial y de producto** (`layers`, `blocks`, `points`, `parcels`, `fields`, `kpis`, `blocks_config`) | La aplicación principal y el servicio Python | **Alto** — es el 90 % del intercambio |
| **Administración** (`users`, `tasks`, `task_details`, `contacts`, `relations`, `clients_data`, `invitations`, `domains`, `resource_projects`, `resources`, …) | La aplicación principal | Bajo — normalmente sólo lectura |
| **Propias del servicio Python** (`model_inferences`, `reprocess_jobs`) | Sólo el servicio Python | **Alto** — la aplicación principal las ignora |

---

## 2. Conexión y convenciones de acceso

### 2.1 Conexión

```python
from pymongo import MongoClient

client = MongoClient(MONGO_STRING, w='majority')
db = client['main']          # la base es SIEMPRE "main"
```

- La base de datos es `main`. No hay bases por tenant: el aislamiento se hace con el campo
  `repo_id` (ver §3.1).
- `w='majority'` es recomendable: con *w=1* se pueden perder escrituras ante un failover, y
  entonces el servicio Python puede leer un estado que la otra aplicación cree haber guardado.
  Si no se puede usar *majority*, usar al menos `w=1` de forma consciente.

### 2.2 Identificadores

- `_id` es un **ObjectId** (`bson.ObjectId`).
- **Casi todas** las referencias entre documentos son **strings** con el hex del ObjectId:
  `space_id`, `page`, `parent_id`, `content`, `comments`, `users`, `alias`, `tasks`,
  `properties.parcel_id`, `operation_task_id`, `raster_id`, `report_id`, `layer_id`…
- Excepción: `created_by.user`, `updated_by.user`, `deleted_by.user` también son strings hex
  (no ObjectId). Ver §3.2.

```python
from bson import ObjectId

doc = db.parcels.find_one({"_id": ObjectId(parcel_id)})   # sí: ObjectId
doc = db.layers.find_one({"space_id": parcel_id})        # sí: string
doc = db.layers.find_one({"_id": parcel_id})             # NUNCA: no casa
```

### 2.3 Fechas

- Las fechas se guardan como **BSON Date** (milisegundos desde epoch, UTC).
- `pymongo` las devuelve como `datetime` **naive en UTC** salvo que se use un `CodecOptions` con
  tz-aware. Si se comparan con `datetime.now(timezone.utc)` hay que normalizar:

```python
from datetime import datetime, timezone

def aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
```

- Al insertar, pasar siempre `datetime` con zona horaria explícita (`timezone.utc`). Un `datetime`
  naive se interpreta como UTC por el driver, pero es una fuente clásica de errores de un día.

#### 2.3.1 Excepción: las fechas dentro de `blocks.properties` son **strings** ISO-8601

Este es el punto que más confunde al leer un documento real. No todas las fechas de la base son
BSON Date; depende de **dónde** están:

| Ubicación | Tipo real en MongoDB | Por qué |
|---|---|---|
| Campos de primer nivel: `date_range`, `created_by.time`, `updated_by.time`, `deleted_by.time` | **BSON Date** | Los codifica el encoder BSON de Go directamente. |
| `points.properties.*` (`created_at`, `validated_at`, `validated_at_staff`) | **BSON Date** | `points.properties` es un *struct* tipado (`AlertPointProperties`), no un mapa libre. Excepción: ~20 000 puntos escritos por un proceso externo tienen `created_at` como **string ISO** (§5.4). |
| `kpis.date` | **BSON Date** | `kpis` es un *struct* tipado (`KPI`). |
| **`blocks.properties.*`** (`error_at`, `date_time_max_temp`, `client_feedbacks[].at`, …) | **string ISO-8601** ⚠️ | `blocks.properties` es un mapa libre (`foundation.JSON`). La app Go lo rellena con `json.Marshal` del struct de propiedades, así que `time.Time` se serializa a texto ISO y se queda como `string`. |

Consecuencia práctica: un filtro por rango sobre `blocks.properties.<fecha>` **no casa** con un
`datetime`, hay que comparar contra strings, y un `{"$gte": datetime(...)}` devuelve cero
resultados silenciosamente.

```python
# ❌ no casa: el valor almacenado es un string ISO
db.blocks.find({"block_type": "weather",
                "properties.date_time_max_temp": {"$gte": datetime(2026, 1, 1, tzinfo=timezone.utc)}})

# ✅ hay que comparar contra el string equivalente
db.blocks.find({"block_type": "weather",
                "properties.date_time_max_temp": {"$gte": "2026-01-01T00:00:00Z"}})
```

> 💡 Si necesitas la fecha de un bloque de forma fiable, usa el `date_range` **de primer nivel**
> (`date_range.start_date`), que sí es BSON Date y está siempre presente en informes, alertas,
> operaciones y clima.

---

### 2.4 Excepción: los números dentro de `blocks.properties` son `double` (float64)

Otro punto que confunde al leer un bloque real. `blocks.properties` es un mapa libre
(`foundation.JSON`) y la aplicación principal lo rellena con `json.Marshal` del struct de
propiedades y lo vuelve a leer con `json.Unmarshal` sobre un `map[string]interface{}`. En Go,
`json.Unmarshal` convierte **todo** número JSON en `float64` (no usa `json.Number` salvo que se
pida explícitamente). Al serializar a BSON, `float64` se codifica como **double**.

| Ubicación | Tipo real en MongoDB |
|---|---|
| `blocks.properties.week` | **double** (float64) en la gran mayoría; `int` en una minoría |
| `blocks.properties.year` | **double** en la gran mayoría; `long` (int64) o `int` en una minoría |
| `blocks.properties.level`, `.group`, `.count`, `.health_status` | **double** (float64) |
| `blocks.properties.*` en general | double, salvo campos que el backend escribe como `string` |

Medido sobre los datos reales (base `main`; es una foto de un momento, las cifras cambian):

| Campo | double | int | long | string | ausente |
|---|---|---|---|---|---|
| `blocks.properties.week` | 9030 | 1380 | – | 0 | 24 772 |
| `blocks.properties.year` | 9030 | 14 | 1366 | 0 | 24 772 |
| `blocks.properties.level` | 3628 | 22 | – | 716 (`"info"`) | 30 816 |
| `blocks.properties.group` | 3551 | 8 | – | 839 (`""`) | 30 784 |
| `blocks.properties.count` | 2063 | – | 1 | 0 | 33 118 |
| `blocks.properties.health_status` | 2197 | 1 | – | 0 | 32 984 |

Consecuencias prácticas:

- **Los filtros funcionan igual.** MongoDB compara numéricamente entre tipos: con `week = 43`
  se devuelven los mismos documentos que con `43.0` (y `43` no casa con `"43"`). Por eso un
  `{"properties.week": 26}` sigue encontrándose.
- **La lectura devuelve `float`.** `doc["properties"]["week"]` es `43.0` (un `float` de Python),
  no `43`. Un `f"{week}"` produce `"43.0"`; un `f"{week:02d}"` revienta con `TypeError`.
- **Normaliza al leer.** Usa `int(...)` explícito: `int(doc["properties"]["week"])` → `43`.
  `int()` funciona tanto con `double` como con `int`.
- **No todos los campos numéricos son números.** `properties.level` es el string `"info"` en
  716 bloques de tipo `message`, y `properties.group` es el string `""` en 839 bloques de tipo
  `operation`. Un `int()` a pelo sobre ellos lanza `ValueError`.

> 💡 El mismo viaje por JSON explica la excepción de §2.3.1: un `time.Time` serializado a JSON
> es un texto ISO, así que dentro de `blocks.properties` las fechas son strings y los números son
> `float64`. Fuera de `blocks.properties` (structs tipados: `points.properties`, `kpis`) el
> encoder BSON escribe los tipos nativos y no hay conversión.

---

## 3. El bloque común: campos heredados por todos los documentos

Todas las colecciones con modelo de la aplicación principal (las 23 de §4) comparten un mismo bloque
de campos de base, embebido en el documento. Es imprescindible conocerlo: define la tenancy y la
auditoría.

### 3.1 Identidad y tenancy

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `_id` | ObjectId | Identificador único. |
| `repo_id` | string | **Tenant / cliente.** Identificador del dominio o cliente propietario del documento. Es el único mecanismo de aislamiento entre clientes. |
| `origin_repo_id` | string | Tenant de origen cuando el documento se clona o se importa. |
| `parent_id` | string | Hex del ObjectId del documento padre (string, **no** ObjectId). |
| `family_id` | string | Agrupa documentos relacionados dentro de la misma colección. |
| `source_id` | string | Identificador en el sistema de origen. |
| `source_type` | string | `""`, `imported_data` o `domain_clone`. |
| `external_id` | string | Identificador en un sistema externo (sincronización). |
| `sync_at` | date | Última sincronización. |
| `language` | string | Idioma del documento, p. ej. `es-ES`. |
| `labels` | [string] | Etiquetas: `client`, `enterprise`, `field`, `plot`, `domain`, `user`, `system`, `staff`, `admin`, `template`, `global`, `hidden`, `procesable`, `queued`, `draft`, `error`, `completed`… |
| `tags` | [{key, value, group?}] | Etiquetas libres con clave/valor. |
| `version` | int32 | Se incrementa en cada actualización realizada por la aplicación principal. |
| `notes` | string | Notas internas. |

> **Tenancy.** La aplicación principal añade el filtro `repo_id` a sus **lecturas** (y a sus
> borrados lógicos) siempre que el modelo tenga `repo_id` establecido. Las **actualizaciones** de
> un documento concreto se hacen por `_id` **sin comprobar el tenant**, de modo que el aislamiento
> real depende de que el `_id` sea el correcto. Un consumidor Python que lea **sin** filtrar por
> `repo_id` verá documentos de todos los clientes. Cuando el consumidor es un servicio interno
> (procesamiento geoespacial, IA) esto suele ser lo deseado; cuando el consumidor final expone
> datos al cliente final, hay que filtrar explícitamente.

### 3.2 Auditoría

Los cuatro campos de auditoría tienen **la misma forma**:

```json
{ "user": "66a7a3c2fef995522871a9a0", "time": ISODate("2026-10-21T08:15:00Z") }
```

| Campo BSON | Tipo | Significado |
|---|---|---|
| `created_by` | {user, time} | Quién creó el documento y cuándo. |
| `updated_by` | {user, time} | Quién hizo la última modificación y cuándo. |
| `deleted_by` | {user, time} | Marca de **borrado lógico**. La usan `domains` y `users` (ver §7.4). |
| `last_access` | {user, time} | Último acceso registrado. |
| `locked_by` | {user, time} | Documento bloqueado por un usuario. |

Detalles importantes:

- `user` es el **hex del ObjectId del usuario**, guardado como **string**. No es un ObjectId
  embebido. Consultarlo como ObjectId (`{"created_by.user": ObjectId(...)}`) no encuentra nada.
- **`created_by` ausente ⇒ el documento se considera nuevo.** La aplicación principal decide
  entre insertar o actualizar comprobando exactamente eso. Si un consumidor Python inserta un
  documento sin `created_by`, la siguiente actualización que haga la aplicación principal sobre
  ese mismo `_id` **insertará un duplicado** en lugar de actualizarlo. Ésta es la trampa más
  grave de esta base de datos.
- La aplicación principal borra de forma **física** en la mayoría de colecciones (ver §7.4); el
  campo `deleted_by` sólo tiene efecto real en `domains` y `users`.

### 3.3 Campos que la aplicación principal no persiste

Algunos campos existen en la API y en los modelos pero **no llegan nunca a MongoDB** por parte de la
aplicación principal. Si Python los escribe, quedan almacenados pero la otra aplicación los ignora
al leer: no sirven para sincronizar estado.

| Campo | Observación |
|---|---|
| `touched` | La aplicación principal ni lo lee ni lo escribe. Es habitual que el servicio Python lo escriba, pero no usarlo como señal de "documento modificado". |
| `labels_not` | Filtro de consulta efímero, nunca se guarda. |
| `date_range` en `layers` | No se persiste en documentos de capa (sí en `blocks` y `points`). Si Python lo escribe en una capa, la aplicación principal lo ignora. |

---

## 4. Catálogo de colecciones

### 4.1 Núcleo (interés alto para Python)

| Colección | Contenido | Escritura | Lectura típica desde Python |
|---|---|---|---|
| `layers` | Geometrías geoespenciales: parcelas (`plot`), sectores (`sector`), embalses (`dam`) | App principal (plot/sector) y Python (dam) | `layer_type`, `space_id`, `properties.*`, `geometry` |
| `blocks` | Bloques de contenido: informes, alertas, meteorología, operaciones, tareas de operación, configuración, **parcelas (`space`/`parcel`)**, capas de producto (`layer`) | App principal | `block_type`, `space_id`, `page`, `properties.*` |
| `blocks_config` | Configuraciones de bloques (`block_type: "config"`) | App principal | `properties.config.*` |
| `points` | Puntos de alerta/predicción (GeoJSON Feature) | App principal y Python | `space_id`, `page`, `properties.*` |
| `parcels` | Parcelas (fincas) | App principal | `name`, `repo_id`, `status` |
| `fields` | Explotaciones agrícolas | App principal | `name`, `parcels[]`, `status` |
| `kpis` | Índices de vegetación y volúmenes de embalse | App principal y Python | `space_id`, `report_id`, `layer_id`, `band` |
| `users` | Usuarios de la plataforma | App principal | `nick`, `username`, `labels` |

### 4.2 Administración (interés bajo)

| Colección | Contenido |
|---|---|
| `tasks` | Tareas del sistema |
| `task_details` | Detalle de tarea |
| `contacts` | Contactos de clientes |
| `relations` | Relaciones entre entidades |
| `clients_data` | Datos agregados de cliente (SSO) |
| `invitations` | Invitaciones a clientes |
| `domains` | Dominios / repositorios. **Borrado lógico.** |
| `plots` | Plots sueltos |
| `resource_projects` | Proyectos |
| `project_tasks` | Tareas de proyecto |
| `resources` | Recursos |
| `resource_assignments` | Asignaciones de recursos |
| `portal_upload_sessions` | Sesiones de subida del portal |
| `portal_uploads_reports` | Informes de subida del portal |
| `blocks_bin` | Bloques eliminados (papelera). Contiene documentos con la **misma forma** que `blocks`; al vaciarse una ubicación, los bloques se mueven aquí. |
| `logs` | Bitácora de la aplicación. Escrita directamente por el driver, **sin** el bloque de campos base de §3. |
| `contact_forms` | Formularios de contacto del SSO. Base propia (distinta de `main`), **sólo lectura**, y tampoco sigue el bloque de §3. |

### 4.3 Propias del servicio Python

| Colección | Contenido |
|---|---|
| `model_inferences` | Registro de ejecuciones de inferencia |
| `reprocess_jobs` | Colas de reprocesado masivo |

---

## 5. Detalle por colección

### 5.1 `layers` — geometrías geoespenciales

**Qué es.** Un documento por geometría. La colección mezcla varios "tipos" de capa discriminados
por `layer_type`. Los documentos son GeoJSON Features con metadatos.

**Campos propios** (además del bloque común de §3):

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `type` | string | `"Feature"`. |
| `geometry` | {type, coordinates} | GeoJSON. `type` ∈ `Point`, `LineString`, `Polygon`, `MultiPolygon`… `coordinates` es un array de floats o de arrays (según el tipo). |
| `crs` | string \| {type, properties{name}} | `"EPSG:4326"` o el documento GeoJSON completo. Ambos formatos conviven en la base. |
| `layer_type` | string | `plot`, `sector`, `crop`, `irrigation` (gestionados por la app principal) o `dam`, `dam_surface` (gestionados por Python). El enum del backend Go sólo reconoce los cuatro primeros: `dam` y `dam_surface` son **exclusivos de MLCore**. |
| `layer_type_sub` | string | Sub-tipo: `crop`, `irrigation`. |
| `space_id` | string | Hex del ObjectId de la **parcela** a la que pertenece la capa. |
| `layer_group` | string | Agrupación de capas. |
| `layer_name` | string | Nombre legible. |
| `active_from` | date | Fecha de activación. |
| `properties` | document | Payload específico del `layer_type`. Ver abajo. |

> ⚠️ El modelo Go declara `DateRange` con `bson:"-"` en la capa: la app principal **no persiste**
> `date_range` en documentos de `layers` (sí lo hace en `blocks` y `points`). Es la razón de la
> trampa #5 de §8.

**`properties` para `layer_type: "plot"`** (parcelas):

| Clave | Tipo | Notas |
|---|---|---|
| `parcel` | string | Nombre de la parcela. Se consulta con regex case-insensitive. |
| `parcel_id` | string | Hex del ObjectId de la parcela en `parcels`. |
| `client` | string | Nombre/código del cliente. |
| `crops` | [string] | Cultivos presentes. |
| `operating` | bool | La parcela está en explotación. Es el filtro de negocio más usado. |
| `operating_snapshot` | bool \| null | Última instantánea conocida de `operating`. |
| `pre_cascade_operating` | bool | Estado previo a una cascada de desactivación. |
| `pending_approval` | bool | Pendiente de aprobación. |
| `poligono`, `recinto`, `zona`, `provincia`, `region`, `altitud`, `agregado`, `municipio`, `parcela` | int | Datos catastrales. |
| `uso_sigpac` | string | Uso SIGPAC. |
| `dn_surface`, `pendiente_media`, `coef_admisibilidad`, `coef_regadio`, `dn_perimeter` | float | Superficie, pendiente, coeficientes. |
| `incidencias` | string | Texto libre. |

**Patrones de consulta (Python)**

```python
# Parcelas (plots) de una parcela concreta
plotas = list(db.layers.find({
    "layer_type": "plot",
    "properties.parcel_id": parcel_id,
}))

# Solo las parcelas en explotación
plotas = list(db.layers.find({
    "layer_type": "plot",
    "properties.parcel_id": parcel_id,
    "properties.operating": True,
}))

# Buscar por nombre de parcela, sin distinguir mayúsculas
plotas = list(db.layers.find({
    "layer_type": "plot",
    "$or": [
        {"properties.parcel": {"$regex": "^" + nombre + "$", "$options": "i"}},
        {"properties.parcel_id": parcel_id},
    ],
}))

# Geometrías que intersectan un extent (requiere índice 2dsphere)
bbox = box(*extent_4326).__geo_interface__
plotas = list(db.layers.find({
    "layer_type": "plot",
    "geometry": {"$geoIntersects": {"$geometry": bbox}},
}))

# Embalses registrados por el servicio Python
embalses = list(db.layers.find({"layer_type": {"$in": ["dam", "dam_surface"]},
                                "space_id": parcel_id}))

# Evitar insertar una geometría que ya solape con otra
if db.layers.find_one({"layer_type": "dam",
                       "geometry": {"$geoIntersects": {"$geometry": geom}}}):
    ...  # ya existe
```

**Notes for writing**

```python
nuevo = {
    "type": "Feature",
    "geometry": geom_geojson,          # dict con "type" y "coordinates"
    "crs": "EPSG:4326",                # string, no documento
    "layer_type": "dam",
    "space_id": parcel_id,             # hex de parcels._id
    "repo_id": repo_id,
    "properties": {...},
    "created_by": {"user": SYSTEM_TOKEN, "time": datetime.now(timezone.utc)},
    "updated_by": {"user": SYSTEM_TOKEN, "time": datetime.now(timezone.utc)},
}
db.layers.insert_one(nuevo)
```

> ⚠️ `repo_id` **no** está en la lista de campos propios de `layers` porque lo hereda del bloque
> común. Igual para `parcels`, `fields`, `kpis` y `blocks`.

---

### 5.2 `blocks` — bloques de contenido

**Qué es.** La colección más heterogénea del sistema. Cada documento es un "bloque" (informe,
alerta, operación, meteorología, configuración…) dentro de un árbol de contenidos. El contenido
es **libre** y su esquema depende de `block_type`; todo lo específico está dentro de `properties`.

**Campos propios** (además del bloque común):

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `block_type` | string | Tipo del bloque. Ver la lista completa en §5.2.1. |
| `block_type_sub` | string | Sub-tipo. Sólo tiene sentido para algunos `block_type` (p. ej. `space` + `parcel`). |
| `title` | string | Título. |
| `space_id` | string | Hex del ObjectId de la parcela/espacio. |
| `page` | string | Hex del ObjectId del bloque padre (p. ej. el informe al que pertenece una operación). |
| `book`, `shelf`, `library`, `section`, `group` | string | Ancestros del bloque. **No son `block_type`**: son campos de referencia. |
| `status` | string | Estado del bloque (`pending`, `active`, `inactive`, `deleted`…). |
| `content` | [string] | Hex de los ObjectId de los bloques hijos. En las operaciones, aquí van los `operation_task`. |
| `content_archive` | [string] | Hijos archivados. |
| `comments` | [string] | Comentarios (hex de bloques de comentario). |
| `users`, `alias`, `tasks`, `notifications` | [string] | Referencias a otros bloques. |
| `date_range` | {start_date, end_date} | Rango de fechas del bloque. **Sí se persiste** (a diferencia de `layers`). |
| `properties` | document | Payload específico del `block_type`. |
| `params` | document | Parámetros libres con booleanos. |
| `search` | string | Texto indexado para búsqueda. |
| `permission_id` | string | Referencia de permisos. |
| `permissions` | [{id, permission_type, value, label}] | Nube de permisos (embebida en el bloque). |
| `repo_tags` | [{key, value, group?}] | Etiquetas con ámbito de tenant. |
| `key` | string | Clave lógica. |
| `icon` | string | Icono. |
| `original_creator` | string | Creador original. |
| `last_notification` | date | Última notificación enviada. |
| `time_lines` | [{key, value, group?}] | Entradas de línea temporal. |

#### 5.2.1 Catálogo de `block_type`

Lista completa de los valores que la aplicación principal escribe en `blocks.block_type`:

| Grupo | Valores |
|---|---|
| **Contenido** | `text`, `text_labeled`, `file`, `image`, `pdf`, `link`, `container`, `alias`, `template`, `page`, `folder`, `comment`, `message`, `tag`, `item`, `inventory`, `map` |
| **Espacios** | `space` (con `block_type_sub`: `parcel` o `domain`), `parcel`, `plot`, `place` |
| **Producto** | `report`, `alert`, `weather`, `operation`, `operation_task`, `layer`, `feedback` |
| **Administración** | `user`, `user_role`, `user_group`, `staff`, `client`, `contact`, `task`, `document`, `invoice`, `domain`, `virtual`, `touch`, `label_action`, `genesis`, `crop` |
| **Configuración** | `config` (y sus sub-tipos `enum`, `use_case`, `phytopathology`, `model`, `email_recipients`, `mail`) |

Notas:

- `book`, `shelf`, `library`, `section`, `group` **no son `block_type`**: son campos de referencia
  a bloques ancestro (ver la tabla de campos propios).
- `plot` como `block_type` (bloque) es distinto de `layers` con `layer_type: "plot"` (geometría de
  parcela). No confundir: el primero es un nodo del árbol de contenidos, el segundo una capa.
- `block_type_sub` sólo está poblado en algunos tipos. Los casos útiles para Python son
  `space` + `parcel` (§5.2.2) y `space` + `domain`.

**`properties` según `block_type`**

`block_type: "report"` (informe semanal):

| Clave | Tipo |
|---|---|
| `parcel_name` | string |
| `parcel_id` | string |
| `parcel_alias` | string |
| `health_status` | **double** (float64) — ver §2.4 |
| `station` | string |
| `week` | **double** (float64) — número de semana ISO. Ver §2.4 |
| `year` | **double** (float64). Ver §2.4 |
| `operation_id` | string |
| `observations` | string |

`block_type: "alert"`:

| Clave | Tipo |
|---|---|
| `level` | **double** (float64) — ver §2.4 |
| `group` | **double** (float64) — ver §2.4 |
| `key` | string |
| `sub_key` | string |
| `points` | [string] — **formato mixto**, ver nota abajo |
| `source` | string |
| `week` | **double** (float64). Ver §2.4 |
| `year` | **double** (float64). Ver §2.4 |
| `count` | **double** (float64) |
| `virtual` | bool |
| `confidence` | float |
| `cluster_confidence` | {string: float} |
| `client_feedbacks` | [{user_id, at, star?, rate_message?, tags?}] |

> ⚠️ **`properties.points` de una alerta tiene dos formatos históricos** (ver §8, trampa 15):
>
> | Formato | Ejemplo | Origen | Qué hacer |
> |---|---|---|---|
> | Coordenadas `"lon, lat"` | `"674224.056, 4221851.925"` | Alertas antiguas (2024-2025) | `float(x), float(y)` tras `split(",")` |
> | Hex de ObjectId | `"69ca73681de64fb3fb0de214"` | Alertas recientes (2025-2026) | Referencia a un documento de `points` |
>
> Reparto real medido: **3931** coordenadas y **5204** hex, sin solapamiento. Los hex **sí**
> resuelven a documentos existentes en `points`. Un parser que sólo entienda un formato descarta
> la mitad de las alertas (884 en total con formato hex).

`block_type: "weather"`:

| Clave | Tipo |
|---|---|
| `year`, `week` | **double** (float64) — ver §2.4 |
| `avg_temp`, `max_temp`, `min_temp` | float |
| `date_time_max_temp`, `date_time_min_temp` | **string ISO-8601** ⚠️ |
| `avg_humidity`, `max_humidity`, `min_humidity` | float |
| `date_time_max_hum`, `date_time_min_hum` | **string ISO-8601** ⚠️ |
| `wind_speed`, `wind_direction`, `max_wind_speed` | float |
| `wind_dir_max_speed` | float — dirección de la racha máxima. |
| `date_time_max_speed` | **string ISO-8601** ⚠️ |
| `radiation`, `precipitation`, `et_pmon`, `pe_pmon` | float |
| `station`, `station_weitec`, `station_type` | string |
| `last_error` | string |

> ⚠️ **No existe `properties.date` en los bloques de clima.** Ni a nivel de `properties` ni a nivel
> de primer nivel: el único campo `date` de todo el modelo Go está en `kpis.date`. Para ordenar
> los bloques de clima por fecha usa el `date_range.start_date` **de primer nivel** (BSON Date), no
> `properties.date`:
>
> ```python
> # ❌ properties.date no existe: el sort no ordena nada y devuelve un bloque arbitrario
> db.blocks.find_one({"block_type": "weather", "space_id": parcel_id},
>                    sort=[("properties.date", -1)])
>
> # ✅ date_range.start_date es BSON Date y sí ordena
> db.blocks.find_one({"block_type": "weather", "space_id": parcel_id},
>                    sort=[("date_range.start_date", -1)])
> ```
>
> `properties.week` / `properties.year` sí son numéricos y sí se pueden filtrar por rango, pero
> llegan como `float64` (§2.4): normaliza con `int(...)` al leer.

`block_type: "operation"` (vuelo de una finca):

| Clave | Tipo | Notas |
|---|---|---|
| `folder` | string | **Id del informe/carpeta generado por la operación.** Clave de unión con `blocks` de tipo `report`. |
| `target_repo_id` | string | Tenant destino. |
| `target_space_id` | string | Parcela destino. |
| `parcel_id`, `parcel`, `parcel_alias` | string | Parcela. `parcel_alias` es el código corto interno (sólo staff). |
| `results` | [{key, value}] | Resultados de la operación. `key` ∈ `report_id`, `index`, `mosaic_hd`, `hd`, `hdmulti`, `mosaic_multi`, `ndvi`, `gndvi`, `ndre`, `lowres`, `lowres_coverage`. |
| `week`, `year` | **double** (float64) | Semana y año del vuelo. Ver §2.4 |
| `week_year` | string | Semana compacta, p. ej. `2611`. |
| `progress`, `merge_retries` | int | |
| `last_state`, `actual_task`, `reference`, `operator`, `link`, `merge_status` | string | |
| `op_type` | string | `""`, `mosaic` (vuelo normal) o `emergency` (vuelo de emergencia). |
| `notes` | string | |
| `staff`, `linked`, `incidents` | [string] | |
| `metadata` | document | |
| `camera_rgb`, `camera_multi` | string | |
| `merged`, `need_merge` | bool | |
| `merge_groups` | [document] | Plan de merge progresivo. |
| `fields` | [{id, name}] | **Legacy**, pero presente en ~72 % de las operaciones. Ver nota abajo. |

> ⚠️ **`properties.fields` es un campo legacy que la aplicación principal ya no escribe.**
> Históricamente era un array de las explotaciones de la finca; la línea que lo rellenaba está
> comentada en el backend Go
> (`block_operation_service.go`: `// Fields: []models.MiniModel{{ID: parcel.FieldID, Name: parcel.FieldName}}`)
> y el tipo `MiniModel` ya no existe en el modelo. Pero **las operaciones viejas sí lo tienen**: en
> los datos reales **1735 de 2416** operaciones (≈72 %) lo conservan. Ojo a dos detalles:
>
> - Las claves del JSON real son **minúsculas** (`{"id": ..., "name": ...}`), no `ID` / `Name`.
>   Los valores de `name` son nombres de explotación (`"Albatera"`, `"El Prao"`).
> - Un filtro `{"properties.fields": {"$elemMatch": {"name": ...}}}` **no casa con operaciones
>   nuevas** (las 681 que no lo tienen): devuelve cero y el fallback queda muerto.
> - Para el nombre de la finca usa `properties.parcel` (o `properties.parcel_id`), que sí se
>   rellenan siempre.

`block_type: "operation_task"`:

| Clave | Tipo |
|---|---|
| `op_type` | string (`visible`, `multispectral`, `thermal`) |
| `camera_key` | string |
| `server` | string |
| `task_id` | string |
| `steps` | [{id, status, op_step, progress, actions[]}] — `op_step` ∈ `mosaic`, `clip`, `index`, `model_prediction` |
| `results` | [{key, value}] — mismos `key` que en `operation` |
| `progress` | float |
| `files` | int |
| `link`, `link_result`, `link_folder`, `last_state`, `reference`, `source` | string |
| `verified` | bool |
| `retry_count`, `max_retries` | int |
| `last_error`, `error_type` | string |
| `error_at` | **string ISO-8601** ⚠️ |

> ⚠️ `op_type` es `multispectral` (sin guion bajo), **no** `multi_spectral`. Un filtro con el valor
> equivocado devuelve cero resultados sin error.

#### 5.2.2 `block_type: "space"` — parcelas y dominios

Un bloque `space` es un **nodo del árbol de contenidos que representa una parcela o un dominio**.
Es la representación *estructural* de la parcela, distinta de la colección `parcels` (que es el
registro de negocio con nombre, alias, estado…). Es la relación muchos-a-uno que hace posible
consultar "todas las parcelas de un cliente".

**`block_type_sub` distingue los dos casos:**

| `block_type` | `block_type_sub` | Qué representa |
|---|---|---|
| `space` | `parcel` | Una parcela. |
| `space` | `domain` | El dominio (tenant) de un cliente. |

**`properties` para `block_type: "space"`**

| Clave | Tipo | Notas |
|---|---|---|
| `parent_space_id` | string | Hex del ObjectId del espacio padre (p. ej. el `domain`). |
| `space` | document | Contenido libre del espacio. |
| `members` | [{user_id, space_role}] | Miembros y su rol en el espacio (`owner`, `admin`, …). |
| `default_permissions` | [{id, permission_type, value, label}] | Permisos por defecto del espacio. |

```python
# Catálogo de parcelas del tenant (parcelas en el árbol de contenidos)
parcel_blocks = list(db.blocks.find({"block_type": "space",
                                     "block_type_sub": "parcel",
                                     "repo_id": repo_id},
                                    {"repo_id": 1, "title": 1}))

# El _id de un bloque space/parcel es el space_id que usan layers, blocks y points
space_id = str(parcel_blocks[0]["_id"])
```

> 💡 Es la vía más fiable para **enumerar todas las parcelas de un cliente**, porque `parcels` es la
> colección de negocio y `blocks` (space/parcel) es la que la aplicación principal usa para el
> árbol de contenidos y para propagar estados en cascada. Las dos vistas existen para la misma
> parcela y comparten el mismo `_id`.

#### 5.2.3 `block_type: "layer"` — capas de producto dentro de un informe

Un bloque `layer` describe un **producto ráster derivado** de una operación (mosaico, clip, índice),
dentro del árbol de contenidos de un informe. No es lo mismo que la colección `layers` (§5.1), que
guarda geometrías.

**`properties` para `block_type: "layer"`**

| Clave | Tipo | Notas |
|---|---|---|
| `layer_type` | string | `mosaic_hd`, `mosaic_multi`, `hd`, `hdmulti`, `ndvi`, `ndre`, `gndvi`, `lowres`, `lowres_coverage`. |
| `high_detail` | bool | |
| `operation_id` | string | Hex del ObjectId de la `operation` que produjo la capa. |
| `operation_task_id` | string | Hex del ObjectId de la `operation_task` concreta. |
| `operation_status` | string | |
| `progress` | float | |

```python
# Raster hd de un informe (mosaico RGB)
capas = list(db.blocks.find({"block_type": "layer",
                              "space_id": parcel_id,
                              "properties.layer_type": "hd"}))
```

**Patrones de consulta (Python)**

```python
# Informe por id
report = db.blocks.find_one({"_id": ObjectId(report_id)})

# La operación que generó un informe (por carpeta)
op = db.blocks.find_one({"block_type": "operation",
                         "properties.folder": report_id})

# Tareas de operación de una operación concreta
tasks = db.blocks.find({"block_type": "operation_task",
                        "page": str(op["_id"])})

# Raster ids "hdmulti" producidos por las tareas de una operación
for tb in db.blocks.find({"block_type": "operation_task",
                          "page": str(op["_id"])},
                         {"properties.results": 1}):
    for r in (tb.get("properties") or {}).get("results") or []:
        if r.get("key") == "hdmulti" and r.get("value"):
            ...

# Alertas de una parcela en una semana/año
alertas = list(db.blocks.find({
    "block_type": "alert",
    "space_id": parcel_id,
    "properties.week": week,
    "properties.year": year,
}))

# Meteorología: la más reciente
# OJO: properties.date NO existe. Se ordena por date_range.start_date (BSON Date).
clima = db.blocks.find_one({"block_type": "weather", "space_id": parcel_id},
                           sort=[("date_range.start_date", -1)])

# Parcelas del cliente según el árbol de contenidos
parcelas = list(db.blocks.find({"block_type": "space",
                                 "block_type_sub": "parcel",
                                 "repo_id": repo_id}))
```

---

### 5.3 `blocks_config` — configuración

**Qué es.** Configuraciones de bloques. El documento tiene la misma forma base que `blocks` pero
con campos extra:

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `values` | [{key, value, group?}] | Valores admitidos. |
| `category` | string | Categoría de la configuración. |
| `parent_config_id` | string | Configuración padre. |
| `aliases` | [string] | Alias. |
| `config` | document | Payload de configuración (campo de primer nivel). |
| `properties.config` | document | **El mismo payload, accesible dentro del documento libre `properties`.** Es la ruta que consulta el consumidor Python. |

El resto de campos (`block_type`, `space_id`, `page`, `book`…, `properties`) son los de `blocks`.

```python
cfg = db.blocks_config.find_one(
    {"block_type": "config", "properties.config.app": "mlcore"},
    {"properties.config": 1},
)
if not cfg or "config" not in cfg.get("properties", {}):
    raise RuntimeError("Configuración MLCore no encontrada en blocks_config")
config = cfg["properties"]["config"]
```

---

### 5.4 `points` — puntos de alerta y predicción

**Qué es.** Un documento GeoJSON Feature por punto detectado o predicho. Es la colección que más
escribe el servicio Python.

**Campos propios** (además del bloque común):

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `type` | string | `"Feature"`. |
| `geometry` | {type, coordinates} | GeoJSON. |
| `crs` | string \| {type, properties{name}} | |
| `repo_id` | string | Tenant. |
| `space_id` | string | Hex del ObjectId de la parcela. |
| `page` | string | **Hex del ObjectId del informe** al que pertenece el punto. |
| `source_point` | string | Origen del punto: `weifield`, `MLCore`, `platform`. |
| `operation_task_id` | string | Tarea de operación que lo originó. |
| `raster_id` | string | Raster de origen. |
| `properties` | document | Payload del punto (ver abajo). |
| `date_range` | {start_date, end_date} | Rango de fechas. **Sí se persiste.** |

**`properties` (payload del punto)**

> 💡 A diferencia de `blocks.properties`, el `properties` de un punto es un *struct* tipado
> (`AlertPointProperties`), no un mapa libre serializado por JSON. Por eso **las fechas aquí sí son
> BSON Date** (`created_at`, `validated_at`, `validated_at_staff`), a diferencia de lo que ocurre
> dentro de `blocks.properties` (§2.3.1). Y por eso los números **no** pasan por `float64`:
> aquí `severity`, `weight`, `cluster` son `int` de verdad.

> ⚠️ **Excepciones medidas en `points.properties`:**
>
> | Campo | Tipos reales | Detalle |
> |---|---|---|
> | `created_at` | `date` (521 355) · `string` (20 075) | Los strings son de un proceso externo (`created_by.user = "roturas_model"`, `created_by.time` también string ISO, sin `space_id`), de 2026-04-27 a 2026-05-22. |
> | `validated_at` | `null` (534 200) · `date` (18) · ausente (7212) | El servicio Python escribe `null` explícito (`datetime \| None = None` en `PointProperties`). |
> | `week` | `string` (500 642) · ausente (40 788) | ⚠️ En bloques es numérico; aquí es string. |

| Clave | Tipo | Notas |
|---|---|---|
| `title` | string | |
| `label` | string | |
| `confidence` | float | |
| `aspect_ratio`, `width_m`, `height_m` | float | Métricas de la detección. |
| `px_xmin`, `px_ymin`, `px_xmax`, `px_ymax` | float | Bounding box en píxeles. |
| `comments` | string | |
| `severity` | int | Gravedad. |
| `use_case` | string | `plagas_y_enfermedades`, `salud_del_cultivo`, `roturas_hidricas`, `""`. |
| `use_case_detail` | string | `trips`, `acaro_oriental`, `arana_roja`, `roturas_hidricas`, `""`. |
| `model_name` | string | Modelo que generó la predicción. |
| `source_version` | string | Versión del modelo. |
| `source_name` | string | `MLCore`, `platform`, `weifield`… |
| `week` | **string** | Semana compacta (`"2611"`). ⚠️ En bloques es numérico (`double`); aquí es string. |
| `crops` | [string] | Cultivos afectados. |
| `weight` | int | Peso de la detección. |
| `hide` | bool | Oculto en el cliente. |
| `position_source` | string | |
| `horizontal_accuracy` | float | Precisión GPS (m). |
| `parcel_id`, `parcel_name` | string | |
| `created_at` | date \| **string ISO** ⚠️ | Fecha de creación. String en ~20 000 puntos de un escritor externo (ver la nota de arriba). |
| `created_by` | **string** | Hex del usuario. ⚠️ Es un string y **no** es lo mismo que el `created_by` de primer nivel. |
| `photos_ids` | [{id, file_repo_type, description, created_at, synced}] | Sólo metadatos; el binario vive en el repositorio de ficheros. |
| `validated_at` | date \| null \| ausente | El servicio Python escribe `null` cuando no está validado. |
| `validated_by` | string | |
| `validated_correct` | bool | |
| `validated_comment` | string | |
| `validated_tags` | [string] | |
| `validated_at_staff` | date | |
| `validated_by_staff` | string | |
| `validated_correct_staff` | bool | |
| `validated_comment_staff` | string | |
| `key` | string | |
| `heatmap` | bool | Marcado como heatmap. |
| `cluster` | int | Cluster al que pertenece. |
| `highest` | bool | Punto más alto del cluster. |

**Patrones de consulta (Python)**

```python
# Puntos de un informe, de un caso de uso concreto
puntos = list(db.points.find({
    "page": {"$in": report_ids},
    "properties.use_case": "roturas_hidricas",
}))

# Puntos creados por un conjunto de usuarios (el user es un string hex)
puntos = list(db.points.find({
    "properties.source_name": "weifield",
    "created_by.user": {"$in": samplers},        # samplers = [hex, ...]
}))

# Reemplazar las predicciones de un modelo para un informe
db.points.delete_many({
    "space_id": parcel_id,
    "page": report_id,
    "properties.model_name": model_name,
    "operation_task_id": operation_task_id,
})
db.points.insert_many(nuevos_puntos)
```

> ⚠️ Al construir un punto, `properties.created_by` (string hex del usuario) es un campo
> **distinto** del `created_by` de primer nivel (`{user, time}`). Si se "normaliza" moviendo
> `properties.created_by` al primer nivel, se pierde la referencia de autor que la aplicación
> principal lee desde `properties`.

---

### 5.5 `parcels` — parcelas

**Qué es.** Una fila por finca/parcela. Es la entidad central: casi todas las demás colecciones la
referencian mediante `space_id` (= hex de `parcels._id`).

**Campos propios**:

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `name` | string | Nombre de la parcela. Se busca con regex case-insensitive. |
| `alias` | string | Código interno corto, único global. Sólo staff. |
| `field_id` | string | Hex de la explotación (`fields`). |
| `field_name` | string | Nombre de la explotación. |
| `owner_name`, `province` | string | |
| `domain_id` | string | Dominio al que pertenece. |
| `identification_code` | string | |
| `status` | string | `pending`, `active`, `inactive`, `deleted`, `coordinations`, `baja`. |
| `pre_cascade_status` | string | Estado previo a una cascada de desactivación. |
| `status_changed_at` | date | |
| `status_history` | [{changed_at, from_status, to_status, changed_by_id, changed_by_username}] | Log de cambios de estado. |
| `crops` | [string] | |
| `last_report_id` | string | Último informe generado. |
| `station`, `station_weitec`, `station_type` | string | Estación meteorológica. |
| `center`, `zoom`, `plan` | string | |
| `sectors` | [string] | Sectores de la parcela. |
| `size`, `flown_hectares` | string | |
| `periodicity` | string | Periodicidad de vuelo. |
| `party_role`, `field_owner` | string | |
| `last_notification` | date | |
| `discount` | float | |
| `ice_mode` | bool | |

```python
# Buscar parcela por nombre (case-insensitive)
parcela = db.parcels.find_one({"name": {"$regex": "^" + nombre + "$", "$options": "i"}})
parcel_id = str(parcela["_id"])

# Tenant de una parcela
repo_id = db.parcels.find_one({"_id": ObjectId(parcel_id)})["repo_id"]

# Parcelas en activo de un tenant
activas = list(db.parcels.find({"repo_id": repo_id,
                                "status": "active"}))
```

---

### 5.6 `fields` — explotaciones

**Qué es.** Una explotación agricola; agrupa parcelas (`parcels[]`) y es la unidad desde la que se
propagan cambios de estado en cascada.

**Campos propios**:

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `name` | string | Nombre de la explotación. |
| `parcels` | [string] | Hex de los ObjectId de las parcelas. |
| `identification_code`, `owner_id`, `owner_name` | string | |
| `country`, `country_code`, `province`, `province_code` | string | |
| `location`, `location_code` | string | |
| `size`, `flown_hectares` | string | |
| `status` | string | `pending`, `active`, `inactive`, `deleted`, `coordinations`, `baja`. |
| `pre_cascade_status` | string | Estado previo a la cascada. |
| `previous_status` | string | Último estado distinto del actual. |
| `status_changed_at` | date | |
| `status_history` | [{changed_at, from_status, to_status, changed_by_id, changed_by_username}] | Log de transiciones. |
| `docs_uploaded`, `coordinations_notified` | bool | |
| `last_notification` | date | |
| `crops` | [string] | |
| `station`, `station_weitec`, `station_type` | string | |
| `center`, `zoom`, `plan` | string | |
| `party_role`, `field_owner` | string | |
| `discount` | float | |

```python
campo = db.fields.find_one({"name": nombre_explotacion})
parcel_ids = campo["parcels"]
```

---

### 5.7 `kpis` — índices y volúmenes

**Qué es.** Métricas numéricas por parcela / informe / capa / banda. Documento plano, sin
geometría.

**Campos propios**:

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `date` | date | Fecha del dato. |
| `space_id` | string | Hex de la parcela. |
| `report_id` | string | Hex del informe. |
| `layer_id` | string | Identificador de la capa/Parcela dentro del informe. **Vacío = agregado a nivel de finca.** |
| `band` | string | `ndvi`, `gndvi`, `ndre`, `""`. |
| `mean`, `std`, `cv`, `min`, `max`, `median` | float | Estadísticos. |
| `percentile_25`, `percentile_75` | float | |
| `count` | int | Número de píxeles. |

**Extensión usada por el servicio Python** (campos adicionales que la aplicación principal no
conoce, pero que conviven en la misma colección):

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `raster_id` | string | Raster del que procede el KPI. |
| `repo_id` | string | Tenant. |
| `kpi_type` | string | `""` para índices de vegetación, `dam` para volúmenes de embalse. |
| `vt`, `Vw`, `Vwp`, `At`, `Aw`, `hs`, `ring_thickness` | float | Volumen de embalse. |
| `labels` | [string] | Etiquetas (p. ej. `embalse`). |

```python
# KPIs de una parcela
kpis = list(db.kpis.find({"space_id": parcel_id}))

# KPIs de un informe y banda
kpis = list(db.kpis.find({"space_id": parcel_id,
                          "report_id": report_id,
                          "band": "ndvi"}))

# Pares (report_id, raster_id, repo_id) distintos ya calculados
pipeline = [
    {"$match": {"space_id": parcel_id, "raster_id": {"$exists": True}}},
    {"$group": {"_id": {"report_id": "$report_id",
                         "raster_id": "$raster_id",
                         "repo_id": "$repo_id"}}},
]
```

> ⚠️ El `cv` que almacena la aplicación principal es `std / mean * 100` (porcentaje). Mantener
> esa convención al escribir KPIs desde Python.

---

### 5.8 `users` — usuarios

**Qué es.** Usuarios de la plataforma. La aplicación principal escribe este documento; desde
Python se consulta (típicamente para resolver `nick` a partir de `created_by.user`).

**Campos persistidos**:

| Campo BSON | Tipo | Descripción |
|---|---|---|
| `nick` | string | Nombre visible. |
| `username` | string | Login. |
| `contact_id` | string | Referencia al contacto. |
| `email` | string | |
| `phone` | string | |
| `preferred_language` | string | |
| `password` | string | Hash. |
| `licenses` | [string] | |
| `connection` | string | |
| `session` | {start_date, end_date} | |
| `external_token` | string | |
| `avatar` | string | |
| `sys_notify` | [string] | Notificaciones suscritas (`report_published`). |
| `rolepermission` | {permission_id, permission_type, role} | |

```python
usuarios = {str(u["_id"]): u["nick"]
            for u in db.users.find({"_id": {"$in": [ObjectId(s) for s in sampler_ids]}},
                                   {"_id": 1, "nick": 1})}
```

---

## 6. Colecciones gestionadas exclusivamente por el servicio Python

Estas colecciones **no** existen en la aplicación principal. Son de propiedad exclusiva del
servicio Python y no hay riesgo de conflicto de formato, pero tampoco hay nada que "respete" la
aplicación Go.

### 6.1 `model_inferences`

Un documento por ejecución de inferencia.

| Campo | Tipo | Notas |
|---|---|---|
| `space_id`, `report_id`, `raster_id`, `operation_task_id`, `week`, `raster_path` | string | Contexto de la ejecución. |
| `type` | string | Tipo de inferencia (`pest_prediction`, `roturas`…). |
| `pest` | string | Plaga objetivo. |
| `status` | string | `pending`, `running`, `finished`, `error`. |
| `error` | string | Mensaje si `status == "error"`. |
| `summary` | document | Totales de la inferencia. |
| `timing` | document | Tiempos en segundos. |
| `config` | document | Configuración usada. |
| `started_at`, `finished_at` | date | |
| `created_by`, `updated_by` | {user, time} | Mismo formato que el resto. |

### 6.2 `reprocess_jobs`

Cola de reprocesado masivo.

| Campo | Tipo | Notas |
|---|---|---|
| `pairs` | [document] | Pares a reprocesar. |
| `indexes` | [string] | Índices a regenerar. |
| `space_id`, `repo_id` | string | Sólo observabilidad. |
| `cursor` | int | Posición de avance. |
| `total`, `processed`, `skipped` | int | Contadores. |
| `errors` | [string] | Errores acumulados. |
| `status` | string | `pending`, `running`, `finished`. |
| `created_at`, `updated_at` | date | |

```python
job = db.reprocess_jobs.find_one_and_update(
    {"_id": ObjectId(job_id),
     "status": {"$ne": "finished"},
     "$or": [{"status": {"$ne": "running"}},
             {"updated_at": {"$lt": stale_cutoff}}]},
    {"$set": {"status": "running", "updated_at": datetime.now(timezone.utc)}},
    return_document=ReturnDocument.AFTER,
)
```

---

## 7. Reglas de escritura

Estas reglas existen para que un documento escrito desde Python siga siendo válido para la
aplicación principal.

### 7.1 Siempre `created_by` y `updated_by`

```python
from datetime import datetime, timezone

def audit(user_hex: str) -> dict:
    return {"user": user_hex, "time": datetime.now(timezone.utc)}

doc["created_by"] = audit(SYSTEM_TOKEN)
doc["updated_by"] = audit(SYSTEM_TOKEN)
```

Un documento sin `created_by` es, para la aplicación principal, un documento **nuevo**. Cuando la
aplicación principal lo actualice, insertará un duplicado en vez de modificarlo.

### 7.2 No tocar `_id`

`_id` es inmutable y es la clave con la que la aplicación principal encuentra el documento. Si se
necesita "recrear" un documento, hacer `insert_one` con un `_id` nuevo y borrar el anterior de
forma explícita, nunca cambiar el `_id` con `$set`.

### 7.3 Actualizaciones parciales con `$set`

```python
db.kpis.update_one(
    {"_id": ObjectId(kpi_id)},
    {"$set": {"mean": 0.42, "updated_by": audit(SYSTEM_TOKEN)}},
)
```

Nunca incluir `_id` dentro de `$set` (MongoDB lo rechaza). Y no usar `replace_one` salvo que se
quiera reescribir el documento completo, porque `replace_one` borra los campos ausentes — incluido
`created_by`, con la consecuencia de §7.1.

La aplicación principal, en cambio, actualiza con un `$set` del **documento completo del modelo**.
De ahí se derivan dos hechos útiles:

- Un campo que Python añada y la aplicación principal no conozca **no se borra** (un `$set` nunca
  elimina claves), pero sí queda congelado con el valor que Python le dio.
- Un campo que la aplicación principal sí conoce se **sobrescribe** con el valor de su modelo en
  cada actualización. Si Python y la aplicación principal escriben el mismo campo, gana la última
  escritura.

### 7.4 Borrado: físico por defecto, lógico en dos colecciones

- **Físico** (el documento desaparece) en la práctica totalidad de las colecciones: `layers`,
  `blocks`, `points`, `kpis`, `parcels`, `fields`, `tasks`, `task_details`, `contacts`, `relations`,
  `blocks_config`, `invitations`, `portal_upload_sessions`, `portal_upload_report`,
  `resource_projects`, `project_tasks`, `resources`, `resource_assignments`.
- **Lógico** (se marca `deleted_by`) en `domains` y en `users`. En `users` el primer borrado es
  lógico y el segundo ya es físico.

```python
# Borrado físico (lo que hace la aplicación principal en la mayoría de colecciones)
db.layers.delete_many({"space_id": parcel_id, "layer_type": "dam"})

# Borrado lógico: sólo tiene efecto en domains y users
db.domains.update_one(
    {"_id": ObjectId(domain_id), "deleted_by": {"$exists": False}},
    {"$set": {"deleted_by": audit(SYSTEM_TOKEN),
              "updated_by": audit(SYSTEM_TOKEN)}},
)
```

> ⚠️ Un `delete_many` físico desde Python hace desaparecer el documento también para la
> aplicación principal, y puede dejar referencias colgantes (`properties.points` de una alerta,
> `parcels[]` de una explotación, `content` de un bloque). Si lo que se quiere es "dar de baja"
> sin romper referencias, actualizar `deleted_by` es la vía que la aplicación respeta en
> `domains` y `users`.

### 7.5 `repo_id` y `space_id` siempre

Todo documento nuevo debe llevar `repo_id` (tenant) y `space_id` (parcela) cuando le apliquen.
Sin `repo_id`, el documento queda fuera del filtro por defecto de las lecturas de la aplicación
principal y es invisible para el cliente correspondiente.

### 7.6 No escribir campos que la aplicación principal no persiste

La aplicación principal marca `touched` y `labels_not` como "no almacenables": no los lee ni los
escribe. Si Python escribe `touched`, el valor queda en el documento pero la otra aplicación lo
ignora; no sirve como señal de "documento modificado". `labels_not` no tiene sentido en ningún
caso: es un filtro de consulta efímero.

### 7.7 `version` e incrementos

La aplicación principal incrementa `version` en cada actualización. Si Python actualiza un
documento, conviene incrementarlo también para no perder el orden de versiones:

```python
db.points.update_one({"_id": oid}, {"$inc": {"version": 1}, "$set": {...}})
```

---

## 8. Trampas y convenciones de tipos

| # | Trampa | Detalle |
|---|---|---|
| 1 | **`created_by` ausente = duplicado** | Ver §7.1. El fallo más caro. |
| 2 | **`created_by.user` es string** | Hex del ObjectId del usuario, no un ObjectId embebido. `{"created_by.user": ObjectId(x)}` no devuelve nada. |
| 3 | **Dos `created_by` distintos en `points`** | El de primer nivel es `{user, time}`; el de `properties.created_by` es un string hex. No confundirlos ni moverlos. |
| 4 | **`properties.week` cambia de tipo** | En `blocks` (report/alert/weather/operation) es **numérico** (`double` por lo general, ver §2.4); en `points.properties.week` es **string** (`"2611"`). Un filtro `{"properties.week": 26}` no casa con `"26"`. |
| 5 | **`date_range` no existe en `layers`** | En capas no se persiste. En `blocks` y `points` sí, con `start_date` / `end_date`. |
| 6 | **`touched` no se persiste** | La aplicación principal lo ignora. |
| 7 | **`properties` es un documento libre** | Se consulta con notación de punto (`properties.parcel`), pero su contenido depende de `layer_type` / `block_type`. No hay esquema fijo. |
| 8 | **`space_id` es string hex** | `str(parcels._id)`. Un `ObjectId` en el filtro no casa. |
| 9 | **`_id` es ObjectId** | `str(_id)` no casa en filtros por `_id`; sí sirve para construir referencias. |
| 10 | **`crs` con dos formatos** | `"EPSG:4326"` o `{"type": "name", "properties": {"name": "EPSG:4326"}}`. Normalizar al leer. |
| 11 | **El borrado es físico, salvo en `domains` y `users`** | Ver §7.4. Un `delete_many` desde Python elimina el documento para todo el mundo. |
| 12 | **`labels` es un array de strings** | `tags` y `repo_tags` son arrays de `{key, value, group?}`. |
| 13 | **Fechas naive en UTC** | `pymongo` devuelve `datetime` naive en UTC. Normalizar antes de comparar. |
| 14 | **`geometry.coordinates` con formas distintas** | Array de floats en `Point`, array de arrays en `Polygon`. El driver Go acepta ambas; no asumir una sola. |
| 15 | **`properties.points` de una alerta tiene dos formatos** | Array de **hex de ObjectId** (referencia a un documento de `points`, formato reciente) **o** de **coordenadas `"lon, lat"`** (formato antiguo). Reparto real: 5204 hex y 3931 coordenadas, sin solapamiento. Un parser que sólo entienda uno de los dos descarta la mitad de las alertas. Si Python borra puntos de forma física, quedan referencias rotas. Ver §5.2. |
| 16 | **`layer_type` con dos dueños** | `plot`/`sector` los escribe la app principal; `dam`/`dam_surface` los escribe Python. No reescribir los ajenos. |
| 17 | **`properties.date` no existe** | El único campo `date` del modelo es `kpis.date`. Ni en `blocks` (ni a primer nivel) ni en `properties`. Para la fecha de un bloque usa `date_range.start_date` de primer nivel. Un `sort` por `properties.date` no ordena nada y devuelve un bloque arbitrario. |
| 18 | **Las fechas dentro de `blocks.properties` son strings ISO** | `blocks.properties` es un mapa libre serializado por JSON. `properties.error_at`, `properties.date_time_max_temp`, `properties.client_feedbacks[].at` son **texto**, no BSON Date. Comparar contra `datetime` devuelve cero resultados en silencio. En `points.properties` y en `kpis` sí son BSON Date. Ver §2.3.1. |
| 19 | **`op_type` de `operation_task` es `multispectral`** | No `multi_spectral`. El enum real es `visible` / `multispectral` / `thermal`. |
| 20 | **`properties.fields` de las operaciones es legacy** | El backend Go ya no lo escribe (línea comentada en `block_operation_service.go`), pero **las operaciones viejas sí lo conservan** (1735 de 2416 en los datos reales, ~72 %). Las claves son minúsculas (`id`, `name`). En las 681 operaciones nuevas no está: los filtros sobre él devuelven cero y el fallback queda muerto. Usa `properties.parcel` / `properties.parcel_id`. |
| 21 | **`book`, `shelf`, `library` no son `block_type`** | Son campos de referencia a bloques ancestro. Los `block_type` reales están en §5.2.1. |
| 22 | **Las parcelas del árbol de contenidos son `blocks` de tipo `space`** | `block_type: "space"` + `block_type_sub: "parcel"`. Su `_id` es el `space_id` que usan `layers`, `blocks` y `points`. Es la vía para enumerar las parcelas de un cliente. Ver §5.2.2. |
| 23 | **Los números de `blocks.properties` son `double`** | El viaje por `json.Marshal`/`json.Unmarshal` al mapa libre convierte todo número en `float64`. `doc["properties"]["week"]` devuelve `43.0`, no `43`. `f"{week}"` → `"43.0"`; `f"{week:02d}"` → `TypeError`. Normaliza con `int(...)`. Ver §2.4. |
| 24 | **No todo `properties.level` / `properties.group` es un número** | `properties.level` es el string `"info"` en 716 bloques de tipo `message`; `properties.group` es el string `""` en 839 bloques de tipo `operation`. Un `int()` a pelo lanza `ValueError`. |
| 25 | **`points.properties.created_at` puede ser string** | 20 075 puntos (de un proceso externo, `created_by.user = "roturas_model"`) tienen `created_at` como ISO-8601 en texto en vez de BSON Date. Un filtro por rango con `datetime` no los encuentra. Ver §5.4. |

---

## 9. Checklist rápido

Antes de dar por buena una escritura desde Python:

- [ ] ¿El documento lleva `created_by` y `updated_by` con la forma `{user: hex, time: datetime}`?
- [ ] ¿Lleva `repo_id` y, si aplica, `space_id`?
- [ ] ¿El `_id` no se ha tocado?
- [ ] ¿Se usa `$set` en vez de `replace_one`?
- [ ] ¿Las referencias (`space_id`, `page`, `report_id`) se guardan como string hex?
- [ ] ¿Los `ObjectId` de los filtros se construyen con `ObjectId(...)`?
- [ ] ¿El borrado es físico (como hace la aplicación principal) salvo que el documento sea de
      `domains` o `users` y lo que se quiera sea darlo de baja?
- [ ] ¿Los nombres de campo en `properties` son exactamente los del contrato de §5 (no los del
      `json:` de la API, que pueden diferir)?
- [ ] ¿Las fechas se insertan con zona horaria explícita?
- [ ] ¿Se ha comprobado el **tipo real** del campo de fecha en destino? BSON Date a primer nivel;
      string ISO-8601 dentro de `blocks.properties` (§2.3.1).
- [ ] Si el campo viene de `blocks.properties`, ¿se ha normalizado con `int(...)`? Los números
      llegan como `float64` (§2.4) y algunos campos "numéricos" son strings (`level`, `group`).
- [ ] ¿El documento lleva `version` (int32)? La app principal lo espera en el bloque común y lo
      incrementa en cada actualización (§7.7).
- [ ] ¿Se ha revisado que no se escribe en colecciones propiedad de la otra aplicación
      (`layers` con `layer_type` `plot`/`sector`, `blocks` de tipo `report`/`alert`/`weather`,
      `parcels`, `fields`, `users`)?

---

## 10. Resumen de relaciones

```
parcels (parcela / finca)
   │  _id ──────────────┐
   │                    │
   ├─ space_id ─────────┼──────────────┐
   │                    │              │
   ▼                    ▼              ▼
layers                blocks          points
(capas: plot,         (informes,      (puntos de alerta
 sector, dam)         alertas,        y predicción)
   │                  clima,
   │                  operaciones)
   │                    │
   │                    ├─ properties.points[] ──► points._id   (formato hex, alertas recientes)
   │                    └─ properties.points[] ──► "lon, lat"  (formato coordenadas, alertas antiguas)
   │
   └─ properties.parcel_id ──► parcels._id

blocks (operation)
   ├─ properties.folder ──────► blocks._id  (informe generado)
   └─ content[] ──────────────► blocks._id  (operation_task)

blocks (space / block_type_sub: parcel)   ← la parcela en el árbol de contenidos
   └─ _id ────────────────────► space_id usado por layers, blocks y points
   (parcels._id y este _id son el MISMO identificador)

blocks (layer)
   ├─ properties.operation_id ─► blocks._id  (operation)
   └─ properties.operation_task_id ─► blocks._id  (operation_task)

fields (explotación)
   └─ parcels[] ──────────────► parcels._id

kpis
   ├─ space_id ───────────────► parcels._id
   └─ report_id ──────────────► blocks._id  (informe)

users
   └─ created_by.user ────────► users._id  (como string hex)
```

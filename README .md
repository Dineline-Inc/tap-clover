#tap-clover
This is a [Singer](https://singer.io) tap that reads data from Clover API's and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## How to use it
[Singer](https://www.singer.io/) tap that extracts data from a [clover](https://docs.clover.com/clover-platform/reference) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).


### Install

```bash
$ mkvirtualenv -p python3 tap-clover
$ pip install tap-clover
```

### Configuration
Here is an example of basic config file containing the clover connection credentials, e.g.:

```
{
  "host": "https://api.clover.com",
  "access_token": "d6qw038-c9s0-24c8-d3c8-60863261",
  "merchant_id": "U8KGR88CGX34"
}
```


- **host**:This is clover host name.
- **access_token**:This is the clover access_token which is unique for each and every account.
- **merchant_id**:This is the clover merchant_id which is unique for each and every account.

### Discovery mode

The tap can be invoked in discovery mode to find the available API's and
schema of the respective clover API data:

```bash
$ tap-clover --config config.json --discover

```
A discovered catalog is output, with a JSON-schema description of each API's.

```json
    {
      "stream": "Customers",
      "tap_stream_id": "Customers",
      "schema": {
        "type": "object",
        "selected": false,
        "properties": {
          "href": {
            "type": "string"
          },
          "id": {
            "type": "string"
          },
          "orderRef": {
            "type": "null"
          },
          "merchant": {
            "type": "null"
          },
          "firstName": {
            "type": "string"
          },
          "lastName": {
            "type": "string"
          },
          "marketingAllowed": {
            "type": "boolean"
          },
          "customerSince": {
            "type": "integer"
          },
          "metadata": {
            "type": "null"
          }
        },
        "required": [
          "customerSince",
          "firstName",
          "href",
          "id",
          "lastName",
          "marketingAllowed",
          "merchant",
          "metadata",
          "orderRef"
        ]
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "forced-replication-method": "FULL_TABLE",
            "valid-replication-keys": [
              "id"
            ],
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "href"
          ],
          "metadata": {
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "id"
          ],
          "metadata": {
            "inclusion": "available"
          }
        }
      ]
    },
    {
      "stream": "Employees",
      "tap_stream_id": "Employees",
      "schema": {
        "type": "object",
        "selected": false,
        "properties": {
          "href": {
            "type": "string"
          },
          "id": {
            "type": "string"
          },
          "name": {
            "type": "string"
          },
          "nickname": {
            "type": "string"
          },
          "customId": {
            "type": "string"
          },
          "email": {
            "type": "string"
          },
          "inviteSent": {
            "type": "boolean"
          },
          "claimedTime": {
            "type": "integer"
          },
          "deletedTime": {
            "type": "integer"
          },
          "pin": {
            "type": "string"
          },
          "unhashedPin": {
            "type": "string"
          },
          "role": {
            "type": "string"
          },
          "isOwner": {
            "type": "boolean"
          },
          "orders": {
            "type": "object",
            "properties": {
              "href": {
                "type": "string"
              }
            },
            "required": [
              "href"
            ]
          },
          "merchant": {
            "type": "null"
          }
        },
        "required": [
          "claimedTime",
          "customId",
          "deletedTime",
          "email",
          "href",
          "id",
          "inviteSent",
          "isOwner",
          "merchant",
          "name",
          "nickname",
          "orders",
          "pin",
          "role",
          "unhashedPin"
        ]
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "forced-replication-method": "FULL_TABLE",
            "valid-replication-keys": [
              "id"
            ],
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "deletedTime"
          ],
          "metadata": {
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "merchant"
          ],
          "metadata": {
            "inclusion": "available"
          }
        }
      ]
    }
```

### Property file Creation

In API selection, `tap-clover` consumes the property and looks for API's and fields
have been marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a properties file so that it can be
modified

```bash
$ tap-clover --config config.json --discover > properties.json
```


```json
    {
      "stream": "Customers",
      "tap_stream_id": "Customers",
      "schema": {
        "type": "object",
        "selected": false,
        "properties": {
          "href": {
            "type": "string"
          },
          "id": {
            "type": "string"
          },
          "orderRef": {
            "type": "null"
          },
          "merchant": {
            "type": "null"
          },
          "firstName": {
            "type": "string"
          },
          "lastName": {
            "type": "string"
          },
          "marketingAllowed": {
            "type": "boolean"
          },
          "customerSince": {
            "type": "integer"
          },
          "metadata": {
            "type": "null"
          }
        },
        "required": [
          "customerSince",
          "firstName",
          "href",
          "id",
          "lastName",
          "marketingAllowed",
          "merchant",
          "metadata",
          "orderRef"
        ]
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "forced-replication-method": "FULL_TABLE",
            "valid-replication-keys": [
              "id"
            ],
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "href"
          ],
          "metadata": {
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "id"
          ],
          "metadata": {
            "inclusion": "available"
          }
        }
      ]
    },
    {
      "stream": "Employees",
      "tap_stream_id": "Employees",
      "schema": {
        "type": "object",
        "selected": false,
        "properties": {
          "href": {
            "type": "string"
          },
          "id": {
            "type": "string"
          },
          "name": {
            "type": "string"
          },
          "nickname": {
            "type": "string"
          },
          "customId": {
            "type": "string"
          },
          "email": {
            "type": "string"
          },
          "inviteSent": {
            "type": "boolean"
          },
          "claimedTime": {
            "type": "integer"
          },
          "deletedTime": {
            "type": "integer"
          },
          "pin": {
            "type": "string"
          },
          "unhashedPin": {
            "type": "string"
          },
          "role": {
            "type": "string"
          },
          "isOwner": {
            "type": "boolean"
          },
          "orders": {
            "type": "object",
            "properties": {
              "href": {
                "type": "string"
              }
            },
            "required": [
              "href"
            ]
          },
          "merchant": {
            "type": "null"
          }
        },
        "required": [
          "claimedTime",
          "customId",
          "deletedTime",
          "email",
          "href",
          "id",
          "inviteSent",
          "isOwner",
          "merchant",
          "name",
          "nickname",
          "orders",
          "pin",
          "role",
          "unhashedPin"
        ]
      },
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "forced-replication-method": "FULL_TABLE",
            "valid-replication-keys": [
              "id"
            ],
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "deletedTime"
          ],
          "metadata": {
            "inclusion": "available"
          }
        },
        {
          "breadcrumb": [
            "properties",
            "merchant"
          ],
          "metadata": {
            "inclusion": "available"
          }
        }
      ]
    }
```
### API's Selection on properties.jon file

In properties file mark the required API as ` "selected": true` to retrieve the specific API data.
If we mark ` "selected": false` then that API will not return data.


```json
{
      "stream": "Customers",
      "tap_stream_id": "Customers",
      "schema": {
        "type": "object",
        "selected": true,
        "properties": {
          "href": {
            "type": "string"
          },
          "id": {
            "type": "string"
          },
```

### How to run singer tap with property file.

With a properties catalog that describes field and API selections the tap can be invoked:

```bash
$ tap-clover --config config.json --properties properties.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.



### How to run the singer tap with target


```bash
$ tap-clover --config config.json --properties properties.json | target-stich --config config.json
```

We can choose any of the singer target ,according to singer target specification we want to give the connection details in the config file.

### List of Square API"s

List of square API is available inside [Clover_API_list.md](Clover_API_list.md)

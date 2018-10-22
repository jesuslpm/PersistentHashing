# PersistentHashing
<strong>Lightning fast persistent hash tables backed by memory mapped files</strong>

This library will contain a collection of classes that will implement
persistent hash tables. 

Two algorithms will be used in PersistenHashing:
  * Robin Hood Hashing.
  * Separate Chaining and Linear Hashing.

Hash tables will have different features and constraints:
  * Fixed key size and fixed value size.
  * Fixed key size and variable value size.
  * Variable key size and variable value size.
  * Fixed capacity (static hash table).
  * Variable capacity (dynamic hash table).
  * Thread safe.
  * Thread unsafe.

The plan is starting with a thread unsafe persistent hash table using Robin Hood Hashing with fixed capacity,
key size and value size, and end with a concurrent persistent hash table with variable capacity and sizes.

It's not planned to make them crash proof.

# StaticFixedSizeHashTable<TKey, TValue>

This is the most basic persistent hash table. It uses Robin Hood Hashing. It's capacity, key size and value size are fixed.
`TKey` and `TValue` are value types, they must not be reference types and must not contain any reference type members at any level of nesting.

It's roughly as fast as `Dictionary<TKey, TValue>` while being persistent and memory efficient.

The following line of code:

```csharp
var dic = new Dictionary<long, long>(200_000_000);
```

Throws `OutOfMemofyException` on my computer. However the following does not:


```csharp
var hashTable = new StaticFixedSizeHashTable<long, long>(filePath, 200_000_000);
```



Here you have some benchmarks results comparing `Dictionary` and `StaticFixedSizeHashTable`:


```
Dictionary sequential access benchmark
Added 10,000,000 items to Dictionary in 00:00:00.1778698
 
Dictionary random access benchmark
Added 10,000,000 items to Dictionary in 00:00:02.2427423
 
HashTable sequencial access benchmark
Added 10,000,000 items to HashTable in 00:00:00.2956129
HashTable MaxDistance:  1
HashTable flushed in 00:00:00.3712881
Read 10,000,000 items from HashTable in 00:00:00.2086666
 
HashTable random access benchmark
Added 10,000,000 items to HashTable in 00:00:02.1242235
HashTable MaxDistance:  11
HashTable flushed in 00:00:00.6129888
Read 10,000,000 items from HashTable in 00:00:01.4045045
```




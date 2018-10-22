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
Dictionary sequential access
Added 10,000,000 items to Dictionary in 00:00:00.2661790
Read 10,000,000 items from HashTable in 00:00:00.1848652

Dictionary random access
Added 10,000,000 items to Dictionary in 00:00:02.4767011
Read 10,000,000 items from Dictionary in 00:00:02.1444105

StaticFixedSizeHashTable sequencial access
Added 10,000,000 items to HashTable in 00:00:00.3919535
HashTable MaxDistance:  0
HashTable MeanDistance:  0.0
HashTable flushed in 00:00:00.3573758
Read 10,000,000 items from HashTable in 00:00:00.2249386

StaticFixedSizeHashTable random access
Added 10,000,000 items to HashTable in 00:00:02.2854182
HashTable MaxDistance:  10
HashTable MeanDistance:  0.7
HashTable flushed in 00:00:00.6148517
Read 10,000,000 items from HashTable in 00:00:01.8649375
```




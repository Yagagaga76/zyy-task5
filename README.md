### 作业5

1. 统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从大到小输出。

   设计思路：

   **StockMapper 类**

      - **数据预处理**：通过 `split()` 方法将 CSV 格式的每行数据分割成列。这里使用了一个正则表达式：`split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$))`，确保即使在逗号被包含在引号内时，也不会错误分割。
      - **提取股票代码**：假设股票代码是 CSV 行的最后一列，通过数组的最后一个元素获取并去除可能的引号和空格。
      - **输出格式**：将股票代码作为 `key`，值为常量 `1`，传递给 `context.write()`，用于后续统计。

   **StockReducer 类**

      - **输入格式**：每个 `key`（股票代码）会对应一个包含多次 `1` 的 `values` 列表，表示该股票代码在文件中出现的次数。
      - **计数累加**：在 `reduce()` 方法中，通过迭代 `values` 列表，将每个股票代码的次数累计并存入 `countMap`（一个 `TreeMap`），其中 `key` 是股票代码，`value` 是计数。

   ![28d704a7f804e78786e1711af37173f](C:\Users\26292\Documents\WeChat Files\wxid_qdwkkyj6c0l922\FileStorage\Temp\28d704a7f804e78786e1711af37173f.png)

2. 统计数据集热点新闻标题（“headline”列）中出现的前100个⾼频单词，按出现次数从⼤到⼩输出。

    **TokenizerMapper 类**

      - **map() 方法**：
        - 将输入的每一行数据拆分，假设每行的第二列为标题，使用正则表达式去除非字母字符（如标点符号等），并将所有单词转为小写。
        - 将标题中的单词拆分为单词数组，并过滤掉停用词和长度小于2的无效单词。
        - 将符合条件的单词（非停用词、有效单词）输出为键，值为 `1`，交给 Reducer 进行处理。

   **IntSumReducer 类**

      - **reduce() 方法**：对每个单词的值（多个 `1`）进行累加，得到每个单词的总计数。
      - **计数存储**：在 `reduce()` 方法中，将每个单词及其计数存入 `countMap`（一个 `TreeMap`），`TreeMap` 自动按照字母顺序存储键。
      - **cleanup() 方法**：
        -  `reduce()` 操作完成后，通过 `cleanup()` 方法对存储在 `countMap` 中的单词按计数进行降序排序。输出排名前100个最常用的单词及其出现次数。

   ![41029b307a9d888198da3c1332dd5a6](C:\Users\26292\Documents\WeChat Files\wxid_qdwkkyj6c0l922\FileStorage\Temp\41029b307a9d888198da3c1332dd5a6.png)

   ![70e4d2c3b5a9b26cec868e91b3feaa3](C:\Users\26292\Documents\WeChat Files\wxid_qdwkkyj6c0l922\FileStorage\Temp\70e4d2c3b5a9b26cec868e91b3feaa3.png)

（这里有几次跑出来结果不对 所以能看到运行但是重新跑了）

- 主要问题：

在第一个wordcount中，出现了很奇怪的问题

```java
PPG"	1
PPG-AkzoNobel"	2
PPH	20
PPHM	75
PPHMP	6
PPI Data And More"	2
PPL	116
```

返回的不是一个股票代码，而是很奇怪的行，经过排查发现因为没有正确获得csv最后一列的值，导致了前面列的值有混入，所以修改了一下获取`stock`列的代码：`split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$))`。先保证去掉符号，然后获得最后一个单词

- 可修改的地方：

在第二个问题里，我最终获得的单词刚开始会有单个英文字母，我思考后发现是因为我先去掉了数字 就导致表示Q1这样的时间单词，会因为去掉1之后被识别成q。没有想到很好的解决方案 ，最后添加了单词长度要大于1这个判断语句。

但是会不会有就是单词长度超过1，其实想`VS`这种，我是否应该算作结果中间一个呢？


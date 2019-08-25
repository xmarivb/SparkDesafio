# Semantix Desafio Engenheiro de Dados


* Qual o objetivo do comando **cache** em Spark?

O Spark tem dois tipos de operações: transformações e ações. As operações do tipo transformações são lazy. Então essas transformações só são de fato executadas quando o resultado delas for requerido. Em geral esse comportamento deixa o Spark mais eficiente, pois ele não precisa guardar em memoria um grande volume de dados. Porém, pode não ser adequado quando se precisa acessar o resultado desta transformação repetidas vezes, pois a transformação teria que ser repetida a cada requisição. Neste caso é recomendado utilizar o cache do Spark, que vai armazenar o resultado da transformação em memória, tornando mais rápido o acesso ao resultado. 


---

* O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

O MapReduce acessa o disco após a execução de cada tarefa. Isso aumenta o tempo de I/O. Já o Spark processa os dados em memória sem precisar voltar ao disco entre cada tarefa, apenas no final.

Além disso, como o Spark possui o cache, os processamentos que fazem repetidos acessos aos mesmos dados se tornam mais rápidos.  


---

* Qual é a função do **SparkContext**?

O SparkContext conecta a aplicação com o processamento Spark. É ele que coordena os processos de um cluster. É através do SparkContext que se tem acesso à API do Spark, desta forma a aplicação realiza o envio de tarefas a serem realizadas pelo cluster.


---

* Explique com suas palavras o que é **Resilient Distributed Datasets** (RDD).

O processamento no Spark é feito de maneira distribuída. Os dados a serem processados são obtidos de um banco de dados ou arquivo HDFS, por exemplo, e são particionados para que possam ser divididos entre os nós do cluster e assim as partes possam ser processadas de maneira paralela. RDD é este conjunto de partes que pode ser distribuído no cluster.


---

* **GroupByKey** é menos eficiente que **reduceByKey** em grandes dataset. Por quê?

O reduceByKey é mais eficiente pois combina os dados de mesma chave dentro de cada nó executor antes de trafegar os dados para calcular o resultado final. 

Já no groupByKey todos os pares de chave-valor são trafegados para somente no resultado final serem combinados. Quando estamos tratando grandes datasets, significa que uma grande quantidade de valores é trafegada desnecessariamente.


---

* Explique o que o código Scala abaixo faz.
```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

Este código é um contador de palavras.

Ele lê um arquivo de texto armazenado no HDFS. 

Depois ele divide o texto por espaço vazios, ou seja, formando palavras. Então cada palavra formada é colocada dentro de um par chave-valor, onde a palavra é a chave e o valor é 1 (o valor representa que aquela palavra foi encontrada 1 vez).  No fim ele agrupa os pares por chave, somando o valor associado. Desta forma o código agrupa todas as ocorrências de uma palavra somando a quantidade de vezes que ela pareceu no texto.

Por fim o resultado, que seria uma lista de palavras e quantidade de ocorrências, é salvo como um arquivo de texto no HDFS.


---

### Referências

http://spark.apache.org/docs/latest/rdd-programming-guide.html
https://spark.apache.org/examples.html
https://www.infoworld.com/article/3014440/five-things-you-need-to-know-about-hadoop-v-apache-spark.html
https://www.quora.com/What-makes-Spark-faster-than-MapReduce
http://monografias.poli.ufrj.br/monografias/monopoli10020199.pdf
https://docs.microsoft.com/pt-br/azure/hdinsight/spark/apache-spark-overview
https://www.devmedia.com.br/introducao-ao-apache-spark/34178
https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html


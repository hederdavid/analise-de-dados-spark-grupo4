# Análise de Dados Meteorológicos com Scala e Spark

Este é um programa desenvolvido em **Scala** e **Apache Spark** que realiza a leitura de um arquivo contendo informações meteorológicas e executa diversas consultas analíticas. Os resultados são exibidos de forma gráfica utilizando **Plotly**.

## 📋 Propósito
Este projeto foi desenvolvido como parte de um trabalho acadêmico da faculdade, realizado pelo **Grupo 4**.

### 🔗 Acesse o Projeto
- **[Projeto no GitHub Pages](https://hederdavid.github.io/analise-de-dados-spark-grupo4/)**

---

## ⚙️ Tecnologias Utilizadas
- **Scala** (versão 2.13.14)
- **Apache Spark** (versão 3.5.3)
- **Plotly-Scala** (versão 0.8.1)

---

## 🚀 Funcionalidades
O programa realiza as seguintes análises baseadas nos dados meteorológicos:

1. **Temperaturas médias em toda a base de dados**  
   Calcula a temperatura média entre máxima e mínima de todos os registros. Exibe apenas o valor da média.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/1-temperatura_media_geral.html)

2. **Dias com grande variação de temperatura**  
   Seleciona todos os dias em que a diferença entre a temperatura máxima e a mínima foi superior a 20°C. Exibe data, cidade, temperatura máxima e mínima.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/2-frequencia_cidades.html)

3. **Dias mais chuvosos em cada cidade**  
   Seleciona registros com precipitação maior que 50 mm. Exibe data, cidade e precipitação.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/3-frequencia_cidades_chuvosas.html)

4. **Dias de alta umidade e alta temperatura**  
   Lista os registros com temperatura máxima acima de 30°C e umidade superior a 80%. Exibe data, cidade, temperatura máxima e umidade.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/4-dias_alta_umidade_temperatura.html)

5. **Temperatura média máxima em toda a base de dados**  
   Calcula a média das temperaturas máximas de todos os registros. Exibe apenas o valor da média.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/5-media_temperatura_maxima.html)

6. **Dias com vento mais intenso**  
   Seleciona registros com velocidade do vento superior a 20 km/h. Exibe data, cidade e velocidade do vento.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/6-vento_intenso_ocorrencias.html)

7. **Dias com precipitação acima da média**  
   Calcula a precipitação média e seleciona dias com precipitação superior a esse valor médio. Exibe data, cidade e precipitação.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/7-precipitacao_acima_media.html)

8. **Dias de baixa temperatura e vento forte**  
   Filtra dias com temperatura mínima abaixo de 10°C e velocidade do vento acima de 15 km/h. Exibe data, cidade, temperatura mínima e velocidade do vento.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/8-baixa_temperatura_vento_forte.html)

9. **Diferença entre temperaturas máxima e mínima**  
   Identifica cidades com as maiores variações diárias de temperatura.  
   [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/9-maior_variacao_temperatura_por_cidade.html)

10. **Distribuição de umidade ao longo do ano**  
    Determina os trimestres mais úmidos do ano (jan-mar, abr-jun, jul-set, out-dez).  
    [🔗 Gráfico Gerado](https://hederdavid.github.io/analise-de-dados-spark-grupo4/relatorios_grafico/10-umidade_por_trimestre_com_desvio.html)

---

## 🛠️ Instalação

### Pré-requisitos
- **JDK 8** ou **JDK 11**
- Ambiente de desenvolvimento, como o **IntelliJ IDEA**.

### Passo a passo
1. Clone este repositório:
   ```bash
   git clone https://github.com/hederdavid/analise-de-dados-spark-grupo4.git
Abra o projeto no IntelliJ IDEA.

2. Certifique-se de que as dependências no arquivo `build.sbt` estão corretamente configuradas:
    
    ```scala
    name := "Temperatura"
    
    version := "0.1"
    
    scalaVersion := "2.13.14"
    
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
    libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.1"

3. Verifique qual versão do JDK está sendo utilizada. Se estiver usando JDK 17 ou superior, altere para JDK 8 ou JDK 11.
    - Para ajustar a versão do JDK no IntelliJ, vá em **File > Project Structure > Project** e altere a versão para JDK 8 ou JDK 11.

No arquivo `Main.scala`, localize a seção de relatórios e descomente a linha correspondente ao relatório que deseja executar. Use uma linha por vez para gerar os resultados. Exemplo:

```scala
// Relatórios
//relatorios.Relatorio_1.show(dfRenomeado)
relatorios.Relatorio_2.show(dfRenomeado)
//relatorios.Relatorio_3.show(dfRenomeado)
//relatorios.Relatorio_4.show(dfRenomeado)
//relatorios.Relatorio_5.show(dfRenomeado)
//relatorios.Relatorio_6.show(dfRenomeado)
//relatorios.Relatorio_7.show(dfRenomeado)
//relatorios.Relatorio_8.show(dfRenomeado)
//relatorios.Relatorio_9.show(dfRenomeado)
//relatorios.Relatorio_10.show(dfRenomeado)
```
relatorios_gerados: Contém os arquivos HTML com os resultados dos relatórios gerados.
### Equipe
- Heder David
- João Vitor Lemos
- Júlia Carvalho
- Luana Rocha
- Marco Aurélio Silva
- Rebeca Almeida

**Итоговый проект**
 Описание задачи.
 Разработать ETL процесс, получающий ежедневную выгрузку данных  (предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно  строящий отчет.

** Выгрузка данных.**
 Ежедневно некие информационные системы выгружают следующие 
файлы:
 Список транзакций за текущий день. Формат – CSV.
 Список терминалов полным срезом. Формат – XLSX.
 Список паспортов, включенных в «черный список» - с накоплением с 
начала месяца. Формат – XLSX.

**Построение отчета.**
 По результатам загрузки ежедневно необходимо строить витрину отчетности по мошенническим операциям. Витрина строится 
накоплением, 
каждый новый отчет укладывается в эту же таблицу с новым report_dt. 

 Признаки мошеннических операций.
 1.Совершение операции при просроченном или заблокированном  паспорте.
 2. Совершение операции при недействующем договоре.
 3. Совершение операций в разных городах в течение одного часа.
 4. Попытка подбора суммы. В течение 20 минут проходит более 3-х операций со следующим шаблоном – каждая последующая меньше предыдущей, при этом отклонены все кроме последней. Последняя операция (успешная) в 
такой цепочке считается мошеннической.


**ЗАПУСК ПРОГРАММЫ**
При запуске программа просит указать номер запуска от 1 до 3-х в отдельном окне.

Необходимо запускать последовательно т.к. от этого зависит загрузка файлов, сортированных по дате и создание первоначальных таблиц.

все таблицы tmp_ - отнес к STG
таблицы банка (cards, accounts, clients) - DWH_DIM
таблицы transactions, passportblacklist - DWH_FACT
таблица terminals - DWH_DIM_.._HIST
таблица отчета - REP_FRAUD

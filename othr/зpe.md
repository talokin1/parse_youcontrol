Мета

Інтеграція клієнтського зворотного зв’язку (NPS) у головний Retail датасет для підвищення точності моделей та розуміння задоволеності клієнтів.

1. Збір та аналіз даних

Отримано NPS-дані у форматі .xlsx від команди RassakhatskyiO Team.

Проведено попередній аналіз для визначення ключових метрик:

рівень лояльності клієнтів (Promoters / Passives / Detractors);

частота заповнення опитувань;

сегментація за каналами обслуговування (відділення, мобільний банкінг, контакт-центр).

----
Integration of customer feedback (NPS) into the main Retail dataset to improve model accuracy and understanding of customer satisfaction.
1. Data collection and analysis
NPS data was obtained in .xlsx format from the RassakhatskyiO Team.
Preliminary analysis was conducted to determine key metrics:
customer loyalty level (Promoters / Passives / Detractors);
survey completion frequency;
segmentation by service channels (branches, mobile banking, contact center).

Translated with DeepL.com (free version)











----

2. Інтеграція у Retail Dataset

Об’єднання з основним Retail датасетом за клієнтським ідентифікатором.

Формування нових ознак:

nps_score — числовий показник задоволеності;

nps_trend — динаміка зміни NPS у часі;

nps_category — категорія клієнта (Detractor / Passive / Promoter).

Підготовка даних до використання у моделях ML (нормалізація, відсутні значення).


---
Інтеграція у Retail Dataset

Об’єднання з основним Retail датасетом за клієнтським ідентифікатором.

Формування нових ознак:

nps_score — числовий показник задоволеності;

nps_trend — динаміка зміни NPS у часі;

nps_category — категорія клієнта (Detractor / Passive / Promoter).

Підготовка даних до використання у моделях ML (нормалізація, відсутні значення).



---


3. Тестування у DS-моделях

Передача оновленого датасету команді Data Science Retail.

Проведення експериментів:

порівняння старих моделей та оновлених з NPS-ознаками;

оцінка зміни F1-score для кожної моделі.

Очікуваний результат — підвищення точності прогнозування поведінки клієнтів (churn, cross-sell, retention).

Очікуваний ефект

Інтеграція емоційно-поведінкової компоненти у фінансову аналітику.

Підвищення ефективності таргетування клієнтів із низькою лояльністю.

Використання NPS як додаткового індикатора “Golden Client Experience”.



Testing in DS models

Transferring the updated dataset to the Data Science Retail team.

Conducting experiments:

comparing old models and updated ones with NPS indicators;

evaluating the change in F1-score for each model.

Expected result — increased accuracy of customer behavior forecasting (churn, cross-sell, retention).

Expected effect

Integration of emotional and behavioral components into financial analytics.

Improved targeting of customers with low loyalty.

Use of NPS as an additional indicator of “Golden Client Experience.”


Translated with DeepL.com (free version)
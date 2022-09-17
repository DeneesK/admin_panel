# Generated by Django 3.2 on 2022-09-17 12:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0005_auto_20220915_1004'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='filmwork',
            index=models.Index(fields=['creation_date', 'rating'], name='film_work_date_rating_idx'),
        ),
        migrations.AddIndex(
            model_name='personfilmwork',
            index=models.Index(fields=['film_work_id', 'person_id'], name='film_work_person_idx'),
        ),
        migrations.AddConstraint(
            model_name='genrefilmwork',
            constraint=models.UniqueConstraint(fields=('film_work_id', 'genre_id'), name='film_work_genre_idx'),
        ),
    ]
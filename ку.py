print("Лабораторна робота №2 Програмування циклічних алгоритмів")
print('')
print("Виконав студент з групи КМ-31 Бован Миколай(Варіант 4)")
print('')


# 1 завдання
def exercise1():
  while True:
    try:
      print("ЗАВДАННЯ 1")
      print('Знайти суму чисел від i = 1 до n, таку що (x+i)/i')
      print('')
      print('Введіть "exit" щоб вийти у головне меню')
      # Блок де вводяться числа з клавіатури
      # перевірка 1 числа
      while True:
        try:
          n = input('Введіть n: ')
          if n == 'exit':
            return
          n = int(n)
          break
        except ValueError:
          print('Ви ввели не число. Спробуйте ще раз')
      # перевірка 2 числа

      while True:
        try:
          x = input('Введіть x: ')
          if x == 'exit':
            return
          x = int(x)
          break
        except ValueError:
          print('Ви ввели не число. Спробуйте ще раз')

      print('')
      # виконання умов задачі
      sum_ = 0
      for i in range(1, n + 1):
        act = (x + i) / i
        sum_ += act
#Округлення результату
      while True:
        try:
          round_ = input('Округлити результат?(Так/Ні)')
          if round_ == 'exit':
            return
            
          elif round_ == 'Так' or round_ == 'так' or round_ == 'ТАК':
            print('Ваша сума дорівнює:', round(sum_))
            return
            
          elif round_ == 'Ні' or round_ == 'ні' or round_ == 'НІ':
            print('Ваша сума дорівнює:', sum_)
            return

          else:
            continue
        except NameError:
          print('Ви ввели не число. Спробуйте ще раз')
      return
    except ValueError:
      print("Ви ввели не то що потрібно. Введіть ще раз")


# друге завдання
def exercise2():
  # початок 2 завдання

  while True:
    print("ЗАВДАННЯ 2")
    print(
        "Організувати безперервне введення чисел з клавіатури, поки користувач не введе 0. Після введення нуля, показати на екрані кількість чисел, які були введені, їх загальну суму і середнє арифметичне."
    )
    print('')
    print('Введіть "exit" щоб вийти у головне меню')

    sum_ = 0
    n = 0
    while n != 1:
      try:
        x = input('Введіть число: ')
        if x == 'exit':
          return
        elif float(x) == 0:
          print('Ви ввели 0. Кількість введених елементів: 0')
          print('Сумма = 0.')
          print(
              'Середнє арифметичне неможливо визначити(на нуль ділити не можна).'
          )
          print('')
          return
        n += 1
      except ValueError:
        print('Ви ввели не число. Спробуйте ще раз')

    x = float(x)
    counter = 0
    sum_ += x
    while True:
      try:
        counter += 1
        x = float(input('Введіть число: '))
        if x == 0:
          break
        elif str('x') == 'exit':
          return
        sum_ += x
      except ValueError:
        print('Ви ввели не число. Спробуйте ще раз')

    print('Кількість введених цифр: ', counter)
    print('Сумма цифр: ', sum_)
    print('Середнє арифметичне: ', sum_ / counter)
    print('')
    return


# цикл який відповідає за нескінченну роботу програми
while True:
  try:
    number = int(
        input(
            "Оберіть число від 1 до 2, аби переглянути завдання:\n(Щоб вийти з програми введіть 4: )"
        ))

    if number == 1:
      exercise1()
    elif number == 2:
      exercise2()
    elif number == 4:
      break
    else:
      print("Неправильна відповідь. Оберіть значення у запропонованих межах")

  except ValueError:
    print("Ви ввели не то що потрібно. Введіть ще раз")

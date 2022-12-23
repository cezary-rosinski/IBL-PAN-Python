#%% 1
problems = ["32 + 698", "3801 - 2", "45 + 43", "123 + 49"]
problems = ["32 + 8", "1 - 3801", "9999 + 9999", "523 - 49"]

def arithmetic_arranger(problems, print_result=False):
    number_of_problems = len(problems)
    operands = [e.split(' ')[1] for e in problems]
    are_not_digits = [not(el.isdigit()) for sub in [e.split(' ')[::2] for e in problems] for el in sub]
    longest_digit = max([len(el) for sub in [e.split(' ') for e in problems] for el in sub])
    if number_of_problems > 5:
        print('Error: Too many problems')
    elif any(e in operands for e in ['*', '\\']):
        print("Error: Operator must be '+' or '-'")
    elif any(e for e in are_not_digits):
        print("Error: Numbers must only contain digits")
    elif longest_digit > 4:
        print("Error: Numbers cannot be more than four digits")
    else:
        lines_1 = []
        lines_2 = []
        lines_3 = []
        lines_4 = []
        for x in problems:
            a,b,c = x.split(' ')
        
            max_len = len(max(x.split(' '), key=len))
            number_of_dashes = max_len + 2
            line_1 = f'{{:>{number_of_dashes}}}'.format(f'{a}')
            lines_1.append(line_1)
            line_2 = f'{b} {{:>{number_of_dashes-2}}}'.format(f'{c}')
            lines_2.append(line_2)
            line_3 = '-' * number_of_dashes
            lines_3.append(line_3)
            result = eval(x)
            line_4 = f'{{:>{number_of_dashes}}}'.format(f'{result}')
            lines_4.append(line_4)
        
        lines = [lines_1, lines_2, lines_3, lines_4]
        
        if print_result:
            arranged_problems = '\n'.join(['    '.join(e) for e in lines])
        else:
            arranged_problems = '\n'.join(['    '.join(e) for e in lines[:-1]])
    
        return arranged_problems
        

arithmetic_arranger(["32 + 8", "1 - 3801", "9999 + 9999", "523 - a"], True)
arithmetic_arranger(['44 + 815', '909 - 2', '45 + 43', '123 + 49', '888 + 40', '653 + 87'])




print(arranged_problems)


y = '\n'.join(['    '.join([el for el in e.split('\n')]) for e in solved])

print(y)

x = "235 + 52"
eval(x)
x = "32 + 698"
x = "3801 - 2"
x = "45 + 43"
x = "123 + 49"
x = "3 - 1000"
a,b,c = x.split(' ')

max_len = len(max(x.split(' '), key=len))
number_of_dashes = max_len + 2
line_1 = f'{{:>{number_of_dashes}}}'.format(f'{a}')
line_2 = f'{b} {{:>{number_of_dashes-2}}}'.format(f'{c}')
line_3 = '-' * number_of_dashes

total = f'{line_1}\n{line_2}\n{line_3}'

print(total)


arithmetic_arranger(["32 + 698", "3801 - 2", "45 + 43", "123 + 49"])




 #"235 + 52" becomes:

#  235
#+  52
#-----


'''Situations that will return an error:
If there are too many problems supplied to the function. The limit is five, anything more will return: Error: Too many problems.
The appropriate operators the function will accept are addition and subtraction. Multiplication and division will return an error. Other operators not mentioned in this bullet point will not need to be tested. The error returned will be: Error: Operator must be '+' or '-'.
Each number (operand) should only contain digits. Otherwise, the function will return: Error: Numbers must only contain digits.
Each operand (aka number on each side of the operator) has a max of four digits in width. Otherwise, the error string returned will be: Error: Numbers cannot be more than four digits.

If the user supplied the correct format of problems, the conversion you return will follow these rules:
There should be a single space between the operator and the longest of the two operands, the operator will be on the same line as the second operand, both operands will be in the same order as provided (the first will be the top one and the second will be the bottom).
Numbers should be right-aligned.
There should be four spaces between each problem.
There should be dashes at the bottom of each problem. The dashes should run along the entire length of each problem individually. (The example above shows what this should look like.)'''

#%% 2

start_time, time_to_add, start_day = "6:30 PM", "205:12", 'sunday'

def add_time(start_time, time_to_add, start_day=None):
    week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    start_hours = int(start_time.split(':')[0])
    start_minutes = int(start_time.split(':')[-1].split(' ')[0])
    start_in_minutes = start_hours * 60 + start_minutes
    start_period = start_time.split(':')[-1].split(' ')[-1]
    other_period = [e for e in ['AM', 'PM'] if e != start_period][0]
    
    add_time_in_minutes = int(time_to_add.split(':')[0]) * 60 + int(time_to_add.split(':')[-1])
    
    total_time_in_minutes = start_in_minutes + add_time_in_minutes
    final_hours = total_time_in_minutes // 60
    
    whole_day_in_minutes = 24 * 60
    if start_period == 'AM':
        how_many_days = total_time_in_minutes / whole_day_in_minutes
    else:
        how_many_days = (total_time_in_minutes + 12 * 60) / whole_day_in_minutes
    
    hours = [12 for e in range(final_hours // 12)]
    hours.append(final_hours % 12 if final_hours % 12 != 0 else 12)
    
    if len(hours) % 2 == 0:
        final_period = other_period
    else: final_period = start_period
    
    final_minutes = '{:02d}'.format(total_time_in_minutes % 60)
    
    days_later = int(how_many_days // 1)
    if days_later == 0 and not start_day:
        final_time = f'{hours[-1]}:{final_minutes} {final_period}'
    elif days_later == 0 and start_day:
        final_day = start_day
        final_time = f'{hours[-1]}:{final_minutes} {final_period}, {final_day}' 
    elif days_later == 1 and start_day:
        final_day = '(next day)'
        final_week_day = [week[i+days_later] for i, e in enumerate(week) if e.lower() == start_day.lower()][0]
        final_time = f'{hours[-1]}:{final_minutes} {final_period}, {final_week_day} {final_day}'
    elif days_later > 1 and start_day:
        final_day = f'({days_later} days later)'
        final_week_day = [week[i+days_later] if i+days_later <= len(week) else week[(i+days_later)%7] for i, e in enumerate(week) if e.lower() == start_day.lower()][0]
        final_time = f'{hours[-1]}:{final_minutes} {final_period}, {final_week_day} {final_day}'
    elif days_later == 1:
        final_day = '(next day)'
        final_time = f'{hours[-1]}:{final_minutes} {final_period} {final_day}' 
    else:
        final_day = f'({days_later} days later)'
        final_time = f'{hours[-1]}:{final_minutes} {final_period} {final_day}' 
    
    return final_time




add_time("3:00 PM", "3:10")
# Returns: 6:10 PM

add_time("11:30 AM", "2:32", "Monday")
# Returns: 2:02 PM, Monday

add_time("11:43 AM", "00:20")
# Returns: 12:03 PM

add_time("10:10 PM", "3:30")
# Returns: 1:40 AM (next day)

add_time("11:43 PM", "24:20", "tueSday")
# Returns: 12:03 AM, Thursday (2 days later)

add_time("6:30 PM", "205:12", 'sunday')
# Returns: 7:42 AM (9 days later)

add_time("8:16 PM", "466:02", "tuesday")
#"6:18 AM, Monday (20 days later)"



















# Todo

- [x] Gesamte analyse für sämtliche Teams machen und in DB abspeichern, damit nicht immer neu gerechnet werden muss.
- [ ] Vergleich der Abteilungen mit deren Prozent der verlorenen eingegangenen Anrufen.
- [ ] 3 day moving avg für die daily calls über das ganze Jahr. (grapf darüberlegen)
- [ ] daily calls mit eingabe des start und end datum generalisieren (refactor)



# Notes

### Laufzeit-Problem mit Funktion amount_of_calls_each_date

zu viele iterationen:

>for i in range(**30**):                 # Teams
>>for i in range(**2**):              # inbound/outbound
>>>for i in range(**270**):        # daten (Tage)
>>>>for i in range(**2**):      # verbunden/verloren

Total iterationen = **33600**
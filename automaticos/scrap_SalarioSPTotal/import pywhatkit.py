
import pywhatkit as kit
import datetime

# Número de teléfono (con código de país, pero sin el signo +)
phone_number = "+5493794760660"
id_group = "HLDflq1b7Zn3iT4zNSAIhF"
# Mensaje que quieres enviar
message = "Mensaje de test"

# Obtén la hora y los minutos actuales
now = datetime.datetime.now()
hours = now.hour
minutes = now.minute + 1  # Suma 1 minuto al tiempo actual

# Envía el mensaje programado
kit.sendwhatmsg_to_group_instantly(id_group, message)


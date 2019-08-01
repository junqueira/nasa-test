package br.com.nasa.utils

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar


object Data {

		def stringToDate(dtFotoRef:String = dateToString()): Date = {
				val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
				val calendar:Calendar = Calendar.getInstance()
				val date:Date = simpleDateFormat.parse(dtFotoRef)
				calendar.setTime(date)
				date
		}

		def dateToString(date: Date= Calendar.getInstance().getTime, format: String = "yyyyMMdd"): String = {
				val simpleDateFormat:SimpleDateFormat = new SimpleDateFormat(format)
				simpleDateFormat.format(date)
		}

		def getTimestamp(): String ={
				System.currentTimeMillis().toString
		}
}

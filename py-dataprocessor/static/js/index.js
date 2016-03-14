/**
 * Created by abc on 10/01/2016.
 */

$(function(){
	$('#btnProcess').click(function(){
		$.ajax({
			url: '/',
			data: $('form').serialize(),
			type: 'POST',
			success: function(response){
				console.log(response);
			},
			error: function(error){
				console.log(error);
			}
		});
	});
});
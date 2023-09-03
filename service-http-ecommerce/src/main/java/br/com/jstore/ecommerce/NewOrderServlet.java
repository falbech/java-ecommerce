package br.com.jstore.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<String>();

	@Override
	public void init() throws ServletException {
		super.init();
	}
	
	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			// Not caring about security, only setting http as a starting point of a
			// composite workflow.
			var email = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));

			var orderId = UUID.randomUUID().toString();
			var order = new Order(orderId, amount, email);
			orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

			var emailContent = "Thank you for your order, it is currently being processed.";
			emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailContent);
			System.out.println("New order successfully sent.");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("New order successfully sent.");
		} catch (InterruptedException e) {
			throw new ServletException(e);
		} catch (ExecutionException e) {
			throw new ServletException(e);
		}

	}
}

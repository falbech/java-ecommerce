package br.com.jstore.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.jstore.ecommerce.database.LocalDatabase;
import br.com.jstore.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<String>();
	private LocalDatabase database;

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

			var orderId = req.getParameter("uuid");
			var order = new Order(orderId, amount, email);

			try (var database = new OrdersDatabase()) {
				if (database.saveNew(order)) {
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
							new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
					System.out.println("New order successfully sent.");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("New order successfully sent.");

				} else {
					System.out.println("Old order received.");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("Old order received.");

				}
			}

			System.out.println("New order successfully sent.");
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("New order successfully sent.");
		} catch (InterruptedException | SQLException | ExecutionException e) {
			throw new ServletException(e);
		}

	}
}
